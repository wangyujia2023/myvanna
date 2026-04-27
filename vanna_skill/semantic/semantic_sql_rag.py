from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any, Dict, List

from ..doris_client import DorisClient
from ..qwen_client import QwenClient

logger = logging.getLogger(__name__)

_CITY_REPLACEMENTS = {
    "北京": "__CITY__",
    "上海": "__CITY__",
    "广州": "__CITY__",
    "深圳": "__CITY__",
    "杭州": "__CITY__",
    "南京": "__CITY__",
    "苏州": "__CITY__",
    "成都": "__CITY__",
    "重庆": "__CITY__",
    "武汉": "__CITY__",
    "西安": "__CITY__",
    "天津": "__CITY__",
}

_MEMBER_REPLACEMENTS = {
    "PLUS会员": "__PLUS_MEMBER__",
    "PLUS 会员": "__PLUS_MEMBER__",
    "普通会员": "__NORMAL_MEMBER__",
    "VIP会员": "__VIP_MEMBER__",
    "VIP 会员": "__VIP_MEMBER__",
}


def canonicalize_question(question: str) -> str:
    text = (question or "").strip()
    if not text:
        return ""

    normalized = text
    for raw, placeholder in _MEMBER_REPLACEMENTS.items():
        normalized = normalized.replace(raw, placeholder)
    for raw, placeholder in _CITY_REPLACEMENTS.items():
        normalized = normalized.replace(raw, placeholder)

    normalized = re.sub(r"\b\d{4}-\d{2}-\d{2}\b", "__DATE__", normalized)
    normalized = re.sub(r"\b\d{4}-\d{2}\b", "__MONTH__", normalized)
    normalized = re.sub(r"\b\d{4}\b", "__YEAR__", normalized)
    normalized = re.sub(r"一级类目为?\s*\d+", "一级类目为 __CATEGORY_ID__", normalized)
    normalized = re.sub(r"二级类目为?\s*\d+", "二级类目为 __CATEGORY_ID__", normalized)
    normalized = re.sub(r"三级类目为?\s*\d+", "三级类目为 __CATEGORY_ID__", normalized)
    normalized = re.sub(r"\b\d+\b", "__NUM__", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def canonical_hash(question: str) -> str:
    return hashlib.md5((question or "").encode("utf-8")).hexdigest()


def stable_rag_id(canonical_question: str, sql_text: str) -> int:
    digest = hashlib.md5(
        f"{canonical_question}\n{(sql_text or '').strip()}".encode("utf-8")
    ).hexdigest()
    return int(digest[:15], 16)


class SemanticSQLRAGStore:
    def __init__(
        self,
        semantic_client: DorisClient,
        vector_client: DorisClient,
        embed_client: QwenClient,
        *,
        db_name: str,
    ):
        self._sem = semantic_client
        self._vec = vector_client
        self._embed = embed_client
        self._db_name = db_name

    def ensure_table(self) -> None:
        self._sem.execute_write(
            """
            CREATE TABLE IF NOT EXISTS semantic_store.vanna_semantic_sql_rag (
                rag_id BIGINT NOT NULL,
                source_sql_id BIGINT,
                raw_question TEXT,
                canonical_question TEXT,
                sql_text TEXT,
                source VARCHAR(64),
                db_name VARCHAR(100),
                quality_score FLOAT DEFAULT 0,
                embedding ARRAY<FLOAT>,
                canonical_hash VARCHAR(64),
                metadata_json TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=OLAP
            DUPLICATE KEY(rag_id)
            COMMENT 'Semantic 独立 SQL RAG 样本表（参数占位向量检索）'
            DISTRIBUTED BY HASH(rag_id) BUCKETS 4
            PROPERTIES ("replication_num" = "1")
            """
        )

    def rebuild_from_feedback_sources(self) -> Dict[str, int]:
        self.ensure_table()
        rows = self._vec.execute(
            """
            SELECT id, question, sql_text, source, db_name, quality_score, created_at
            FROM vanna_store.vanna_sql
            WHERE source IN ('feedback', 'feedback_corrected')
              AND (db_name = %s OR IFNULL(db_name, '') = '')
            ORDER BY created_at DESC
            """,
            (self._db_name,),
        )

        dedup: Dict[str, Dict[str, Any]] = {}
        for row in rows:
            canonical = canonicalize_question(row.get("question", ""))
            if not canonical:
                continue
            key = f"{canonical}\n{row.get('sql_text', '').strip()}"
            if key not in dedup:
                dedup[key] = {
                    **row,
                    "canonical_question": canonical,
                }

        self._sem.execute_write("DELETE FROM semantic_store.vanna_semantic_sql_rag")

        inserted = 0
        for item in dedup.values():
            try:
                vec = self._embed.get_embedding(item["canonical_question"])
                rag_id = stable_rag_id(
                    item["canonical_question"],
                    item.get("sql_text", ""),
                )
                self._sem.execute_write(
                    """
                    INSERT INTO semantic_store.vanna_semantic_sql_rag
                        (rag_id, source_sql_id, raw_question, canonical_question, sql_text,
                         source, db_name, quality_score, embedding, canonical_hash, metadata_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        rag_id,
                        item.get("id"),
                        item.get("question", ""),
                        item["canonical_question"],
                        item.get("sql_text", ""),
                        item.get("source", ""),
                        item.get("db_name", ""),
                        item.get("quality_score", 0.0) or 0.0,
                        json.dumps(vec),
                        canonical_hash(item["canonical_question"]),
                        json.dumps(
                            {
                                "raw_question": item.get("question", ""),
                                "created_at": str(item.get("created_at", "")),
                            },
                            ensure_ascii=False,
                        ),
                    ),
                )
                inserted += 1
            except Exception as exc:
                logger.warning("[SemanticSQLRAG] rebuild skip question=%s err=%s", item.get("question", "")[:120], exc)

        return {
            "fetched": len(rows),
            "deduped": len(dedup),
            "inserted": inserted,
        }

    def upsert_feedback_sample(
        self,
        question: str,
        sql_text: str,
        *,
        source: str,
        quality_score: float = 1.0,
        source_sql_id: int | None = None,
    ) -> str:
        self.ensure_table()
        canonical = canonicalize_question(question)
        if not canonical or not (sql_text or "").strip():
            return "skipped"
        rag_id = stable_rag_id(canonical, sql_text)
        exists = self._sem.execute(
            """
            SELECT rag_id
            FROM semantic_store.vanna_semantic_sql_rag
            WHERE rag_id = %s
            LIMIT 1
            """,
            (rag_id,),
        )
        if exists:
            return "exists"

        vec = self._embed.get_embedding(canonical)
        self._sem.execute_write(
            """
            INSERT INTO semantic_store.vanna_semantic_sql_rag
                (rag_id, source_sql_id, raw_question, canonical_question, sql_text,
                 source, db_name, quality_score, embedding, canonical_hash, metadata_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                rag_id,
                source_sql_id,
                question,
                canonical,
                sql_text,
                source,
                self._db_name,
                quality_score,
                json.dumps(vec),
                canonical_hash(canonical),
                json.dumps({"raw_question": question}, ensure_ascii=False),
            ),
        )
        return "inserted"

    def search(
        self,
        question: str,
        *,
        top_k: int = 5,
    ) -> List[Dict[str, Any]]:
        self.ensure_table()
        canonical = canonicalize_question(question)
        if not canonical:
            return []
        vec = self._embed.get_embedding(canonical)
        rows = self._sem.execute(
            f"""
            SELECT rag_id, source_sql_id, raw_question, canonical_question,
                   sql_text, source, db_name, quality_score,
                   cosine_distance(embedding, {json.dumps(vec)}) AS dist
            FROM semantic_store.vanna_semantic_sql_rag
            WHERE (db_name = %s OR IFNULL(db_name, '') = '')
            ORDER BY dist ASC, quality_score DESC
            LIMIT {int(top_k)}
            """,
            (self._db_name,),
        )
        for row in rows:
            row["query_canonical_question"] = canonical
            dist = float(row.get("dist", 0) or 0)
            row["similarity"] = max(0.0, min(1.0, 1.0 - dist))
        return rows
