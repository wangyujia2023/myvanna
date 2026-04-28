"""
DorisVanna - 继承 VannaBase，后端全部使用 Doris + Qwen
向量存储：Doris vanna_store 库（cosine_distance）
LLM + Embedding：通义千问（带重试、缓存、降级链）
调用链：每次 generate_sql 均记录到 tracer
"""
import json
import logging
import re
import threading
import time
from typing import Dict, List, Optional, Tuple

import pandas as pd

from vanna.legacy.base import VannaBase

from .doris_client import DorisClient
from .qwen_client import QwenClient
from .tracer import RequestTrace, tracer

logger = logging.getLogger(__name__)

# Doris cosine_distance 值越小越相似（范围 0~2）
_MAX_DISTANCE = 1.0   # 超过此距离认为相关性不足
_ID_LOCK = threading.Lock()
_LAST_ID = 0


def _next_bigint_id() -> int:
    """生成进程内单调递增的 BIGINT 主键，适配 Doris 非自增表。"""
    global _LAST_ID
    with _ID_LOCK:
        candidate = int(time.time() * 1_000_000)
        if candidate <= _LAST_ID:
            candidate = _LAST_ID + 1
        _LAST_ID = candidate
        return candidate


class DorisVanna(VannaBase):
    """
    Vanna Skill 核心实现：
    - 向量检索：Doris vanna_store.vanna_embeddings（cosine_distance）
    - LLM/Embedding：Gemini（带重试、缓存、降级）
    - 每次调用全链路写入 tracer
    """

    def __init__(self, config: dict):
        super().__init__(config=config)
        self._config = config.copy()
        # Qwen（通义千问）
        self._gemini = QwenClient(          # 保持属性名兼容，内部已换成 Qwen
            api_key=config["qwen_api_key"],
            model=config.get("model", "qwen-plus"),
            embedding_model=config.get("embedding_model", "text-embedding-v3"),
            max_concurrent=config.get("max_concurrent", 5),
        )
        # Doris 向量库连接
        self._vec = DorisClient(
            host=config["host"], port=config["port"],
            user=config["user"], password=config.get("password", ""),
            database="vanna_store",
        )
        # Doris 业务库连接
        self._biz = DorisClient(
            host=config["host"], port=config["port"],
            user=config["user"], password=config.get("password", ""),
            database=config.get("database", "retail_dw"),
        )
        self._n_results = config.get("n_results", 5)
        self._db_name = config.get("database", "retail_dw")

    def _persist_trace(self, trace: RequestTrace):
        """将调用链持久化到 Doris，失败不影响主流程。"""
        try:
            self._vec.execute_write(
                """
                INSERT INTO vanna_store.vanna_trace_log
                    (trace_id, question, final_sql, status, model_used,
                     total_ms, error_msg, steps_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    trace.trace_id,
                    trace.question,
                    trace.final_sql,
                    trace.status,
                    trace.model_used,
                    round(trace.total_ms, 1),
                    trace.error,
                    json.dumps([s.to_dict() for s in trace.steps], ensure_ascii=False),
                ),
            )
        except Exception as e:
            logger.warning(f"[DorisVanna] trace 持久化失败: {e}")

    def get_trace_logs(self, n: int = 30) -> List[dict]:
        n = max(1, min(int(n or 30), 30))
        rows = self._vec.execute(
            """
            SELECT trace_id, question, final_sql, status, model_used,
                   total_ms, error_msg, steps_json, created_at
            FROM vanna_store.vanna_trace_log
            ORDER BY created_at DESC
            LIMIT %s
            """,
            (n,),
        )
        result = []
        for row in rows:
            result.append({
                "trace_id": row["trace_id"],
                "question": row["question"] or "",
                "created_at": str(row["created_at"]),
                "status": row["status"] or "",
                "total_ms": float(row["total_ms"] or 0),
                "model_used": row["model_used"] or "",
                "final_sql": row["final_sql"] or "",
                "error": row["error_msg"] or "",
                "steps": json.loads(row["steps_json"] or "[]"),
            })
        return result

    def get_trace_log(self, trace_id: str) -> Optional[dict]:
        rows = self._vec.execute(
            """
            SELECT trace_id, question, final_sql, status, model_used,
                   total_ms, error_msg, steps_json, created_at
            FROM vanna_store.vanna_trace_log
            WHERE trace_id = %s
            LIMIT 1
            """,
            (trace_id,),
        )
        if not rows:
            return None
        row = rows[0]
        return {
            "trace_id": row["trace_id"],
            "question": row["question"] or "",
            "created_at": str(row["created_at"]),
            "status": row["status"] or "",
            "total_ms": float(row["total_ms"] or 0),
            "model_used": row["model_used"] or "",
            "final_sql": row["final_sql"] or "",
            "error": row["error_msg"] or "",
            "steps": json.loads(row["steps_json"] or "[]"),
        }

    # ─────────────────────────────────────────────────────────────────────────
    # VannaBase 抽象方法实现
    # ─────────────────────────────────────────────────────────────────────────

    # ─────────────────────────────────────────────────────────────────────────
    # VannaBase 三个消息格式化抽象方法（OpenAI 风格 role/content dict）
    # ─────────────────────────────────────────────────────────────────────────

    def system_message(self, message: str) -> dict:
        return {"role": "system", "content": message}

    def user_message(self, message: str) -> dict:
        return {"role": "user", "content": message}

    def assistant_message(self, message: str) -> dict:
        return {"role": "assistant", "content": message}

    def generate_embedding(self, data: str, **kwargs) -> List[float]:
        return self._gemini.get_embedding(data)

    def submit_prompt(self, prompt, **kwargs) -> str:
        """LLM 推理入口（被 generate_sql 内部调用）"""
        trace: Optional[RequestTrace] = kwargs.get("_trace")
        step = trace.begin_step("llm_generate", {
            "model": self._gemini.model_name,
            "prompt_len": len(prompt) if isinstance(prompt, str) else "messages",
        }) if trace else None

        try:
            # prompt 可能是字符串或消息列表
            if isinstance(prompt, list):
                text = "\n".join(
                    m.get("content", "") if isinstance(m, dict) else str(m)
                    for m in prompt
                )
            else:
                text = str(prompt)

            result = self._gemini.generate(text)

            if step:
                step.finish(outputs={"response_len": len(result),
                                     "preview": result[:200]},
                            note=self._gemini.model_name)
            return result
        except Exception as e:
            if step:
                step.finish(status="error", error=str(e))
            raise

    # ─────────────────────────────────────────────────────────────────────────
    # 向量检索
    # ─────────────────────────────────────────────────────────────────────────

    def get_similar_question_sql(self, question: str, **kwargs) -> List[Tuple[str, str]]:
        trace = kwargs.get("_trace")
        step = trace.begin_step("vector_search_sql",
                                {"question": question[:60],
                                 "top_k": self._n_results}) if trace else None

        vec = self._gemini.get_embedding(question)
        rows = self._vec.execute(f"""
            SELECT question, content AS sql_text, quality_score,
                   cosine_distance(embedding, {json.dumps(vec)}) AS dist
            FROM vanna_store.vanna_embeddings
            WHERE content_type = 'sql'
              AND quality_score >= 0.3
            ORDER BY dist ASC
            LIMIT {self._n_results}
        """)
        results = [(r["question"], r["sql_text"]) for r in rows
                   if r["dist"] < _MAX_DISTANCE]

        if step:
            step.finish(outputs={
                "found": len(results),
                "top_scores": [round(max(0.0, 1.0 - float(r["dist"] or 0)), 4) for r in rows[:3]],
                "top_distances": [round(float(r["dist"] or 0), 4) for r in rows[:3]],
                "quality_scores": [round(float(r.get("quality_score", 0) or 0), 4) for r in rows[:3]],
                "top_questions": [r["question"][:40] for r in rows[:3]],
            })

        # 更新命中计数
        if results:
            try:
                self._vec.execute_write(f"""
                    UPDATE vanna_store.vanna_embeddings
                    SET use_count = use_count + 1
                    WHERE content_type = 'sql'
                      AND question IN ({",".join(["%s"] * len(results))})
                """, [r[0] for r in results])
            except Exception as e:
                logger.warning(f"[DorisVanna] 跳过 use_count 更新: {e}")

        return results

    def get_related_ddl(self, question: str, **kwargs) -> List[str]:
        trace = kwargs.get("_trace")
        step = trace.begin_step("vector_search_ddl",
                                {"question": question[:60]}) if trace else None

        vec = self._gemini.get_embedding(question)
        rows = self._vec.execute(f"""
            SELECT content,
                   cosine_distance(embedding, {json.dumps(vec)}) AS dist
            FROM vanna_store.vanna_embeddings
            WHERE content_type = 'ddl'
            ORDER BY dist ASC
            LIMIT {self._n_results}
        """)
        results = [r["content"] for r in rows if r["dist"] < _MAX_DISTANCE]

        if step:
            step.finish(outputs={"found": len(results),
                                 "tables": [r["content"][:60] for r in rows[:3]]})
        return results

    def get_related_documentation(self, question: str, **kwargs) -> List[str]:
        vec = self._gemini.get_embedding(question)
        rows = self._vec.execute(f"""
            SELECT content,
                   cosine_distance(embedding, {json.dumps(vec)}) AS dist
            FROM vanna_store.vanna_embeddings
            WHERE content_type = 'doc'
            ORDER BY dist ASC
            LIMIT 3
        """)
        return [r["content"] for r in rows if r["dist"] < _MAX_DISTANCE]

    # ─────────────────────────────────────────────────────────────────────────
    # 训练数据写入
    # ─────────────────────────────────────────────────────────────────────────

    def _source_entry_exists(self, table_name: str, where_sql: str, args: tuple) -> bool:
        rows = self._vec.execute(
            f"SELECT 1 FROM vanna_store.{table_name} WHERE {where_sql} LIMIT 1",
            args,
        )
        return bool(rows)

    def _insert_sql_source(self, row_id: int, question: str, sql: str, source: str, tables: List[str]):
        self._vec.execute_write(
            """
            INSERT INTO vanna_store.vanna_sql
                (id, question, sql_text, source, db_name, table_names)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (row_id, question, sql, source, self._db_name, ",".join(tables)),
        )

    def _insert_doc_source(
        self,
        row_id: int,
        content: str,
        source: str,
        *,
        title: str = "",
        table_names: Optional[List[str]] = None,
    ):
        self._vec.execute_write(
            """
            INSERT INTO vanna_store.vanna_doc
                (id, title, content, source, db_name, table_names)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (row_id, title, content, source, self._db_name, ",".join(table_names or [])),
        )

    def _insert_metadata_source(
        self,
        row_id: int,
        *,
        table_name: str,
        ddl_text: str,
        summary_text: str,
        source: str,
        table_comment: str = "",
        engine: str = "",
        table_rows: int = 0,
    ):
        self._vec.execute_write(
            """
            INSERT INTO vanna_store.vanna_metadata
                (id, table_name, db_name, table_comment, engine, table_rows,
                 ddl_text, summary_text, source)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                row_id,
                table_name,
                self._db_name,
                table_comment,
                engine,
                table_rows,
                ddl_text,
                summary_text,
                source,
            ),
        )

    def _insert_embedding_entry(
        self,
        *,
        row_id: int,
        content_type: str,
        content: str,
        embedding_text: str,
        source: str,
        question: str = "",
        table_names: Optional[List[str]] = None,
    ):
        if question:
            self._vec.execute_write(
                """
                INSERT INTO vanna_store.vanna_embeddings
                    (id, content_type, question, content, embedding,
                     source, db_name, table_names)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row_id,
                    content_type,
                    question,
                    content,
                    embedding_text,
                    source,
                    self._db_name,
                    ",".join(table_names or []),
                ),
            )
        else:
            self._vec.execute_write(
                """
                INSERT INTO vanna_store.vanna_embeddings
                    (id, content_type, content, embedding, source, db_name, table_names)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    row_id,
                    content_type,
                    content,
                    embedding_text,
                    source,
                    self._db_name,
                    ",".join(table_names or []),
                ),
            )

    def _training_entry_exists(
        self,
        content_type: str,
        *,
        content: str,
        question: str = "",
        source: str = "",
        db_name: str = "",
    ) -> bool:
        rows = self._vec.execute(
            """
            SELECT 1
            FROM vanna_store.vanna_embeddings
            WHERE content_type = %s
              AND IFNULL(question, '') = %s
              AND content = %s
              AND IFNULL(source, '') = %s
              AND IFNULL(db_name, '') = %s
            LIMIT 1
            """,
            (content_type, question, content, source, db_name),
        )
        return bool(rows)

    def _sql_source_exists(self, question: str, sql: str, source: str) -> bool:
        return self._source_entry_exists(
            "vanna_sql",
            "IFNULL(question, '') = %s AND sql_text = %s AND IFNULL(source, '') = %s AND IFNULL(db_name, '') = %s",
            (question, sql, source, self._db_name),
        )

    def _doc_source_exists(self, content: str, source: str) -> bool:
        return self._source_entry_exists(
            "vanna_doc",
            "content = %s AND IFNULL(source, '') = %s AND IFNULL(db_name, '') = %s",
            (content, source, self._db_name),
        )

    def _metadata_source_exists(self, table_name: str, source: str) -> bool:
        return self._source_entry_exists(
            "vanna_metadata",
            "table_name = %s AND IFNULL(source, '') = %s AND IFNULL(db_name, '') = %s",
            (table_name, source, self._db_name),
        )

    def _existing_sql_keys(
        self,
        items: List[Dict[str, str]],
        *,
        source: str,
        db_name: str,
        chunk_size: int = 100,
    ) -> set[tuple[str, str]]:
        """
        返回库中已存在的 (question, content) 组合，用于批量去重。
        """
        existing: set[tuple[str, str]] = set()
        if not items:
            return existing

        for i in range(0, len(items), chunk_size):
            chunk = items[i:i + chunk_size]
            clauses = []
            args: list[str] = ["sql", source, db_name]
            for item in chunk:
                clauses.append("(IFNULL(question, '') = %s AND content = %s)")
                args.extend([item.get("question", ""), item["sql"]])

            rows = self._vec.execute(
                f"""
                SELECT IFNULL(question, '') AS question, content
                FROM vanna_store.vanna_embeddings
                WHERE content_type = %s
                  AND IFNULL(source, '') = %s
                  AND IFNULL(db_name, '') = %s
                  AND ({' OR '.join(clauses)})
                """,
                args,
            )
            for row in rows:
                existing.add((row["question"], row["content"]))
        return existing

    def add_question_sql(self, question: str, sql: str, **kwargs) -> str:
        source = kwargs.get("source", "manual")
        if self._sql_source_exists(question, sql, source) or self._training_entry_exists(
            "sql",
            question=question,
            content=sql,
            source=source,
            db_name=self._db_name,
        ):
            return "exists"

        vec = self._gemini.get_embedding(question)
        tables = _extract_tables(sql)
        row_id = _next_bigint_id()
        self._insert_sql_source(row_id, question, sql, source, tables)
        self._insert_embedding_entry(
            row_id=row_id,
            content_type="sql",
            question=question,
            content=sql,
            embedding_text=json.dumps(vec),
            source=source,
            table_names=tables,
        )
        return "ok"

    def add_question_sql_batch(
        self,
        items: List[Dict[str, str]],
        *,
        source: str = "audit_log",
        chunk_size: int = 50,
    ) -> dict:
        """
        批量写入 Q&A 样本：
        - 入参 items: [{"question": "...", "sql": "..."}, ...]
        - 去除本批次重复
        - 过滤库中已存在样本
        - 使用单条 INSERT ... VALUES (...), (...), ... 分块写入
        """
        if not items:
            return {"added": 0, "skipped": 0}

        seen: set[tuple[str, str]] = set()
        deduped: List[Dict[str, str]] = []
        for item in items:
            question = (item.get("question") or "").strip()
            sql = (item.get("sql") or "").strip()
            if not question or not sql:
                continue
            key = (question, sql)
            if key in seen:
                continue
            seen.add(key)
            deduped.append({"question": question, "sql": sql})

        existing = self._existing_sql_keys(
            deduped,
            source=source,
            db_name=self._db_name,
            chunk_size=chunk_size,
        )
        new_items = [
            item for item in deduped
            if (item["question"], item["sql"]) not in existing
        ]

        added = 0
        skipped = len(items) - len(new_items)

        for i in range(0, len(new_items), chunk_size):
            chunk = new_items[i:i + chunk_size]
            values_sql = []
            args = []
            for item in chunk:
                vec = self._gemini.get_embedding(item["question"])
                row_id = _next_bigint_id()
                tables_list = _extract_tables(item["sql"])
                tables = ",".join(tables_list)
                self._insert_sql_source(row_id, item["question"], item["sql"], source, tables_list)
                values_sql.append("(%s, %s, %s, %s, %s, %s, %s, %s)")
                args.extend([
                    row_id,
                    "sql",
                    item["question"],
                    item["sql"],
                    json.dumps(vec),
                    source,
                    self._db_name,
                    tables,
                ])

            self._vec.execute_write(
                f"""
                INSERT INTO vanna_store.vanna_embeddings
                    (id, content_type, question, content, embedding,
                     source, db_name, table_names)
                VALUES {", ".join(values_sql)}
                """,
                args,
            )
            added += len(chunk)

        return {"added": added, "skipped": skipped}

    def add_metadata(
        self,
        *,
        table_name: str,
        ddl: str,
        summary: str,
        source: str = "schema",
        table_comment: str = "",
        engine: str = "",
        table_rows: int = 0,
    ) -> dict:
        if self._metadata_source_exists(table_name, source):
            return {"status": "exists", "ddl": "exists", "doc": "exists"}

        row_id = _next_bigint_id()
        self._insert_metadata_source(
            row_id,
            table_name=table_name,
            ddl_text=ddl,
            summary_text=summary,
            source=source,
            table_comment=table_comment,
            engine=engine,
            table_rows=table_rows,
        )
        ddl_result = "exists"
        if not self._training_entry_exists(
            "ddl",
            content=ddl,
            source=source,
            db_name=self._db_name,
        ):
            ddl_vec = self._gemini.get_embedding(ddl)
            self._insert_embedding_entry(
                row_id=row_id,
                content_type="ddl",
                content=ddl,
                embedding_text=json.dumps(ddl_vec),
                source=source,
                table_names=[table_name],
            )
            ddl_result = "ok"

        doc_result = "exists"
        if summary and not self._training_entry_exists(
            "doc",
            content=summary,
            source=source,
            db_name=self._db_name,
        ):
            doc_row_id = _next_bigint_id()
            doc_vec = self._gemini.get_embedding(summary)
            self._insert_embedding_entry(
                row_id=doc_row_id,
                content_type="doc",
                content=summary,
                embedding_text=json.dumps(doc_vec),
                source=source,
                table_names=[table_name],
            )
            doc_result = "ok"
        return {"status": "ok", "ddl": ddl_result, "doc": doc_result}

    def add_ddl(self, ddl: str, **kwargs) -> str:
        source = kwargs.get("source", "schema")
        table_name = (_extract_tables(ddl) or [""])[0]
        if (table_name and self._metadata_source_exists(table_name, source)) or self._training_entry_exists(
            "ddl",
            content=ddl,
            source=source,
            db_name=self._db_name,
        ):
            return "exists"

        vec = self._gemini.get_embedding(ddl)
        tables = _extract_tables(ddl)
        row_id = _next_bigint_id()
        self._insert_metadata_source(
            row_id,
            table_name=tables[0] if tables else f"unknown_{row_id}",
            ddl_text=ddl,
            summary_text="",
            source=source,
        )
        self._insert_embedding_entry(
            row_id=row_id,
            content_type="ddl",
            content=ddl,
            embedding_text=json.dumps(vec),
            source=source,
            table_names=tables,
        )
        return "ok"

    def add_documentation(self, documentation: str, **kwargs) -> str:
        source = kwargs.get("source", "manual")
        title = kwargs.get("title", "")
        if self._doc_source_exists(documentation, source) or self._training_entry_exists(
            "doc",
            content=documentation,
            source=source,
            db_name=self._db_name,
        ):
            return "exists"

        vec = self._gemini.get_embedding(documentation)
        row_id = _next_bigint_id()
        self._insert_doc_source(row_id, documentation, source, title=title)
        self._insert_embedding_entry(
            row_id=row_id,
            content_type="doc",
            content=documentation,
            embedding_text=json.dumps(vec),
            source=source,
        )
        return "ok"

    # ─────────────────────────────────────────────────────────────────────────
    # 训练数据管理
    # ─────────────────────────────────────────────────────────────────────────

    def get_training_data(self, **kwargs) -> pd.DataFrame:
        return self._vec.query_df("""
            SELECT id, content_type, source, db_name, table_names,
                   question, content_preview, quality_score, use_count, created_at
            FROM (
                SELECT id,
                       'sql' AS content_type,
                       source,
                       db_name,
                       table_names,
                       SUBSTRING(question, 1, 80) AS question,
                       SUBSTRING(sql_text, 1, 120) AS content_preview,
                       quality_score,
                       use_count,
                       created_at
                FROM vanna_store.vanna_sql

                UNION ALL

                SELECT id,
                       'ddl' AS content_type,
                       source,
                       db_name,
                       table_name AS table_names,
                       table_name AS question,
                       SUBSTRING(ddl_text, 1, 120) AS content_preview,
                       quality_score,
                       use_count,
                       created_at
                FROM vanna_store.vanna_metadata
                WHERE IFNULL(ddl_text, '') != ''

                UNION ALL

                SELECT id,
                       'doc' AS content_type,
                       source,
                       db_name,
                       table_names,
                       SUBSTRING(title, 1, 80) AS question,
                       SUBSTRING(content, 1, 120) AS content_preview,
                       quality_score,
                       use_count,
                       created_at
                FROM vanna_store.vanna_doc
            ) t
            ORDER BY created_at DESC
            LIMIT 500
        """)

    def get_sql_source_data(self, limit: int = 300) -> pd.DataFrame:
        return self._vec.query_df(
            f"""
            SELECT id, source, db_name, table_names,
                   SUBSTRING(question, 1, 120) AS question,
                   SUBSTRING(sql_text, 1, 220) AS content_preview,
                   quality_score, use_count, created_at
            FROM vanna_store.vanna_sql
            ORDER BY created_at DESC
            LIMIT {int(limit)}
            """
        )

    def get_doc_source_data(self, limit: int = 300) -> pd.DataFrame:
        return self._vec.query_df(
            f"""
            SELECT id, source, db_name, table_names,
                   SUBSTRING(title, 1, 120) AS question,
                   SUBSTRING(content, 1, 220) AS content_preview,
                   quality_score, use_count, created_at
            FROM vanna_store.vanna_doc
            ORDER BY created_at DESC
            LIMIT {int(limit)}
            """
        )

    def get_metadata_source_data(self, limit: int = 300) -> pd.DataFrame:
        return self._vec.query_df(
            f"""
            SELECT id, source, db_name, table_name AS table_names,
                   table_name AS question,
                   SUBSTRING(ddl_text, 1, 220) AS content_preview,
                   quality_score, use_count, created_at
            FROM vanna_store.vanna_metadata
            ORDER BY created_at DESC
            LIMIT {int(limit)}
            """
        )

    def get_lineage_source_data(self, limit: int = 300) -> pd.DataFrame:
        return self._vec.query_df(
            f"""
            SELECT edge_id AS id, source, relation_type, sql_type,
                   source_table, target_table, freq, created_at
            FROM vanna_store.vanna_lineage
            ORDER BY freq DESC, created_at DESC
            LIMIT {int(limit)}
            """
        )

    def remove_training_data(self, id: str, **kwargs) -> bool:
        affected = 0
        affected += self._vec.execute_write(
            "DELETE FROM vanna_store.vanna_embeddings WHERE id = %s", (id,)
        )
        affected += self._vec.execute_write(
            "DELETE FROM vanna_store.vanna_sql WHERE id = %s", (id,)
        )
        affected += self._vec.execute_write(
            "DELETE FROM vanna_store.vanna_doc WHERE id = %s", (id,)
        )
        affected += self._vec.execute_write(
            "DELETE FROM vanna_store.vanna_metadata WHERE id = %s", (id,)
        )
        return affected > 0

    def update_quality_score(self, id: int, score: float):
        self._vec.execute_write(
            "UPDATE vanna_store.vanna_embeddings SET quality_score=%s WHERE id=%s",
            (score, id)
        )

    # ─────────────────────────────────────────────────────────────────────────
    # SQL 执行（LangChain 层会调用，这里保留给调试使用）
    # ─────────────────────────────────────────────────────────────────────────

    def run_sql(self, sql: str) -> pd.DataFrame:
        return self._biz.query_df(sql)

    # ─────────────────────────────────────────────────────────────────────────
    # 核心对外接口：generate_sql（带完整 trace）
    # ─────────────────────────────────────────────────────────────────────────

    def ask_with_trace(self, question: str,
                       step_callback: Optional[callable] = None) -> dict:
        """
        生成 SQL 并记录完整调用链。
        step_callback(event_type, data): 每步完成后实时回调（用于 SSE 流式推送）
          event_type: 'start' | 'step_start' | 'step_done' | 'final' | 'error'
        返回: {"sql": str, "trace": dict, "error": str}
        """
        trace = tracer.start(question)
        trace.model_used = self._gemini.model_name

        def notify(event_type: str, data: dict):
            if step_callback:
                try:
                    step_callback(event_type, data)
                except Exception:
                    pass

        notify("start", {"trace_id": trace.trace_id, "question": question})

        try:
            # ── Step 1: Embedding ────────────────────────────────────────────
            notify("step_start", {"name": "generate_embedding",
                                  "label": "生成 Embedding 向量"})
            step = trace.begin_step("generate_embedding", {"text": question[:60]})
            was_cached = self._gemini._cache.get(question) is not None
            vec = self._gemini.get_embedding(question)
            step.finish(outputs={"dims": len(vec)},
                        note="CACHED" if was_cached else "FRESH")
            notify("step_done", step.to_dict())

            # ── Step 2: 向量检索 SQL ─────────────────────────────────────────
            notify("step_start", {"name": "vector_search_sql",
                                  "label": "向量检索相似 SQL"})
            step2 = trace.begin_step("vector_search_sql",
                                     {"top_k": self._n_results})
            rows_sql = self._vec.execute(f"""
                SELECT question, content AS sql_text, quality_score,
                       cosine_distance(embedding, {json.dumps(vec)}) AS dist
                FROM vanna_store.vanna_embeddings
                WHERE content_type = 'sql' AND quality_score >= 0.3
                ORDER BY dist ASC LIMIT {self._n_results}
            """)
            sim_sql = [(r["question"], r["sql_text"]) for r in rows_sql
                       if r["dist"] < _MAX_DISTANCE]
            sim_sql_examples = [
                {"question": q, "sql": sql}
                for q, sql in sim_sql
            ]
            step2.finish(outputs={
                "found": len(sim_sql),
                "top_scores": [round(max(0.0, 1.0 - float(r["dist"] or 0)), 4) for r in rows_sql[:3]],
                "top_distances": [round(float(r["dist"] or 0), 4) for r in rows_sql[:3]],
                "quality_scores": [round(float(r.get("quality_score", 0) or 0), 4) for r in rows_sql[:3]],
                "top_questions": [r["question"][:50] for r in rows_sql[:3]],
            })
            notify("step_done", step2.to_dict())

            # ── Step 3: 向量检索 DDL ─────────────────────────────────────────
            notify("step_start", {"name": "vector_search_ddl",
                                  "label": "向量检索相关 DDL"})
            step3 = trace.begin_step("vector_search_ddl")
            rows_ddl = self._vec.execute(f"""
                SELECT content, table_names,
                       cosine_distance(embedding, {json.dumps(vec)}) AS dist
                FROM vanna_store.vanna_embeddings
                WHERE content_type = 'ddl'
                ORDER BY dist ASC LIMIT {self._n_results}
            """)
            rel_ddl = [r["content"] for r in rows_ddl if r["dist"] < _MAX_DISTANCE]
            step3.finish(outputs={
                "found": len(rel_ddl),
                "tables": [r.get("table_names", "")[:40] for r in rows_ddl[:4]],
            })
            notify("step_done", step3.to_dict())

            # ── Step 4: 检索 doc ─────────────────────────────────────────────
            rows_doc = self._vec.execute(f"""
                SELECT content,
                       cosine_distance(embedding, {json.dumps(vec)}) AS dist
                FROM vanna_store.vanna_embeddings
                WHERE content_type = 'doc' ORDER BY dist ASC LIMIT 3
            """)
            rel_doc = [r["content"] for r in rows_doc if r["dist"] < _MAX_DISTANCE]

            # ── Step 5: 组装 Prompt ──────────────────────────────────────────
            notify("step_start", {"name": "build_prompt", "label": "组装 Prompt"})
            step_p = trace.begin_step("build_prompt")
            prompt = self.get_sql_prompt(
                initial_prompt=self._config.get("initial_prompt") or None,
                question=question,
                question_sql_list=sim_sql_examples,
                ddl_list=rel_ddl,
                doc_list=rel_doc,
            )
            prompt_text = "\n".join(
                m.get("content", "") if isinstance(m, dict) else str(m)
                for m in prompt
            )
            step_p.finish(outputs={
                "prompt_len": len(prompt_text),
                "sim_sql_count": len(sim_sql),
                "ddl_count": len(rel_ddl),
                "doc_count": len(rel_doc),
                "initial_prompt": self._config.get("initial_prompt", ""),
                "prompt_full": prompt_text,
                "question_sql_examples": sim_sql_examples,
                "ddl_list": rel_ddl,
                "doc_list": rel_doc,
            })
            notify("step_done", step_p.to_dict())

            # ── Step 6: LLM 推理 ─────────────────────────────────────────────
            notify("step_start", {"name": "llm_generate",
                                  "label": f"LLM 推理 [{self._gemini.model_name}]"})
            step_llm = trace.begin_step("llm_generate", {
                "model": self._gemini.model_name,
                "prompt_len": len(prompt_text),
            })
            llm_response = self._gemini.generate(prompt_text)
            step_llm.finish(
                outputs={"response_len": len(llm_response),
                         "preview": llm_response[:300]},
                note=self._gemini.model_name,
            )
            notify("step_done", step_llm.to_dict())

            # ── Step 7: 提取 SQL ─────────────────────────────────────────────
            notify("step_start", {"name": "extract_sql", "label": "提取 SQL"})
            step_e = trace.begin_step("extract_sql")
            sql = self.extract_sql(llm_response)
            step_e.finish(outputs={"sql": sql[:400]})
            notify("step_done", step_e.to_dict())

            trace.finish(sql=sql)
            self._persist_trace(trace)
            result = {"sql": sql, "trace": trace.to_dict(), "error": ""}
            notify("final", result)
            return result

        except Exception as e:
            trace.finish(error=str(e))
            self._persist_trace(trace)
            logger.exception(f"[DorisVanna] ask_with_trace 失败: {e}")
            result = {"sql": "", "trace": trace.to_dict(), "error": str(e)}
            notify("error", result)
            return result

    # ─────────────────────────────────────────────────────────────────────────
    # 辅助
    # ─────────────────────────────────────────────────────────────────────────

    def generate_question(self, sql: str, **kwargs) -> str:
        """由 SQL 反向生成自然语言问题（用于 audit_log 挖掘）"""
        prompt = (
            f"请用一句简洁的中文提问来描述以下 SQL 的查询目的，"
            f"只输出问题本身，不要解释：\n\n{sql}"
        )
        try:
            return self._gemini.generate(prompt).strip().strip("?？").strip() + "？"
        except Exception:
            return ""

    @property
    def gemini_stats(self) -> dict:
        return self._gemini.stats

    @property
    def tracer_stats(self) -> dict:
        return tracer.stats()


# ── 工具函数 ──────────────────────────────────────────────────────────────────
def _extract_tables(sql_or_ddl: str) -> List[str]:
    """从 SQL/DDL 中提取表名"""
    patterns = [
        r"\bFROM\s+`?(\w+)`?",
        r"\bJOIN\s+`?(\w+)`?",
        r"\bINTO\s+`?(\w+)`?",
        r"\bTABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+)`?",
    ]
    tables = set()
    for pat in patterns:
        for m in re.finditer(pat, sql_or_ddl, re.IGNORECASE):
            t = m.group(1).lower()
            if t not in {"select", "where", "set", "values", "from"}:
                tables.add(t)
    return list(tables)
