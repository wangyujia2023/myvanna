"""
基于 Doris 向量库的多路召回器。

search() 是统一入口：
  - vec 不为 None → 向量检索（cosine_distance）
  - vec 为 None   → 关键词检索（LIKE，降级模式）
"""
from __future__ import annotations

import json
import logging
import re
import threading
from typing import Any, Dict, List, Optional

from ..doris_client import DorisClient
from ..lineage import LineageManager
from ..qwen_client import QwenClient

logger = logging.getLogger(__name__)

# ── 进程级 LineageManager 单例 ───────────────────────────────────────────────
# 跨 AskLCPipeline 实例共享，避免配置变更触发实例重建时重新加载 audit_log。
# 使用 invalidate_lineage_cache() 手动刷新（对应 /lineage/rebuild 接口）。
_global_lineage_manager: LineageManager | None = None
_lineage_lock = threading.Lock()


def get_or_build_lineage(
    biz: DorisClient,
    vec: DorisClient,
    limit: int = 2000,
) -> LineageManager:
    global _global_lineage_manager
    if _global_lineage_manager is not None:
        return _global_lineage_manager
    with _lineage_lock:
        # double-check（另一个线程可能已经建好）
        if _global_lineage_manager is None:
            logger.info("[Lineage] 首次构建血缘图，扫描 audit_log...")
            m = LineageManager(biz, vec)
            table_count = m.lineage_table_count()
            if table_count > 0:
                m.load_graph_from_table()
                logger.info(
                    f"[Lineage] 已从 vanna_lineage 载入血缘图，共 {len(m.graph.edges)} 条边"
                )
            else:
                m.build_from_audit_log(limit=limit)
                m.build_from_vanna_knowledge()
                logger.info(
                    f"[Lineage] 血缘图构建完成，共 {len(m.graph.edges)} 条边"
                )
            _global_lineage_manager = m
    return _global_lineage_manager


def invalidate_lineage_cache():
    """手动使血缘图缓存失效，下次请求时重建（供 /lineage/rebuild 调用）。"""
    global _global_lineage_manager
    with _lineage_lock:
        _global_lineage_manager = None
    logger.info("[Lineage] 血缘图缓存已清除，下次请求将重建")


class DorisRetriever:
    def __init__(
        self,
        vec_client: DorisClient,
        embed_client: QwenClient,
        *,
        db_name: str,
        biz_client: DorisClient | None = None,
        top_k: int = 5,
        max_distance: float = 1.0,
    ):
        self._vec = vec_client
        self._embed = embed_client
        self._biz = biz_client
        self._db_name = db_name
        self._top_k = top_k
        self._max_distance = max_distance
        # 血缘图使用进程级单例，不挂在实例上

    # ─────────────────────────────────────────────────────────────────────────
    # 内部：向量检索
    # ─────────────────────────────────────────────────────────────────────────
    def _search_by_vec(
        self,
        vec: List[float],
        *,
        content_type: str,
        top_k: int,
    ) -> List[Dict[str, Any]]:
        rows = self._vec.execute(
            f"""
            SELECT id, content_type, question, content, source, db_name, table_names,
                   quality_score, use_count,
                   cosine_distance(embedding, {json.dumps(vec)}) AS dist
            FROM vanna_store.vanna_embeddings
            WHERE content_type = %s
              AND (db_name = %s OR IFNULL(db_name, '') = '')
            ORDER BY dist ASC
            LIMIT {top_k}
            """,
            (content_type, self._db_name),
        )
        return [r for r in rows if r.get("dist") is not None and r["dist"] < self._max_distance]

    # ─────────────────────────────────────────────────────────────────────────
    # 内部：关键词检索（降级路径，无向量时使用）
    # ─────────────────────────────────────────────────────────────────────────
    def _search_by_keyword(
        self,
        query: str,
        *,
        content_type: str,
        top_k: int,
    ) -> List[Dict[str, Any]]:
        # 取前 5 个有意义的词（过滤单字、标点）
        words = [w for w in re.split(r"[\s，。？！、,.\?!]+", query) if len(w) > 1][:5]
        if not words:
            # 无有效关键词 → 返回质量最高的 top_k 条
            return self._vec.execute(
                f"""
                SELECT id, content_type, question, content, source, db_name, table_names,
                       quality_score, use_count, NULL AS dist
                FROM vanna_store.vanna_embeddings
                WHERE content_type = %s
                  AND (db_name = %s OR IFNULL(db_name, '') = '')
                ORDER BY quality_score DESC, use_count DESC
                LIMIT {top_k}
                """,
                (content_type, self._db_name),
            )

        # 构造 LIKE 条件：content 或（sql 类型时）question 任意包含一个关键词即命中
        like_clauses = []
        params: list = []
        for w in words:
            pattern = f"%{w}%"
            if content_type == "sql":
                like_clauses.append("(content LIKE %s OR question LIKE %s)")
                params.extend([pattern, pattern])
            else:
                like_clauses.append("content LIKE %s")
                params.append(pattern)

        where_kw = " OR ".join(like_clauses)
        params.extend([content_type, self._db_name])

        rows = self._vec.execute(
            f"""
            SELECT id, content_type, question, content, source, db_name, table_names,
                   quality_score, use_count, NULL AS dist
            FROM vanna_store.vanna_embeddings
            WHERE ({where_kw})
              AND content_type = %s
              AND (db_name = %s OR IFNULL(db_name, '') = '')
            ORDER BY quality_score DESC, use_count DESC
            LIMIT {top_k}
            """,
            params,
        )
        logger.debug(f"[Retriever] keyword fallback: '{query[:40]}' → {len(rows)} rows")
        return rows

    # ─────────────────────────────────────────────────────────────────────────
    # 统一入口：vec=None 自动降级关键词检索
    # ─────────────────────────────────────────────────────────────────────────
    def search(
        self,
        query: str,
        *,
        vec: Optional[List[float]],
        content_type: str,
        top_k: int | None = None,
    ) -> List[Dict[str, Any]]:
        limit = top_k or self._top_k
        if vec is not None:
            return self._search_by_vec(vec, content_type=content_type, top_k=limit)
        return self._search_by_keyword(query, content_type=content_type, top_k=limit)

    # ─────────────────────────────────────────────────────────────────────────
    # 各类型检索（Skill 调用层）
    # ─────────────────────────────────────────────────────────────────────────
    def retrieve_sql_examples(
        self, query: str, *, vec: Optional[List[float]] = None, top_k: int | None = None
    ) -> List[Dict[str, Any]]:
        return self.search(query, vec=vec, content_type="sql", top_k=top_k)

    def retrieve_ddl(
        self, query: str, *, vec: Optional[List[float]] = None, top_k: int | None = None
    ) -> List[Dict[str, Any]]:
        return self.search(query, vec=vec, content_type="ddl", top_k=top_k)

    def retrieve_docs(
        self, query: str, *, vec: Optional[List[float]] = None, top_k: int | None = None
    ) -> List[Dict[str, Any]]:
        return self.search(query, vec=vec, content_type="doc", top_k=top_k)

    def retrieve_audit_patterns(
        self, query: str, *, vec: Optional[List[float]] = None, top_k: int | None = None
    ) -> List[Dict[str, Any]]:
        rows = self.retrieve_sql_examples(query, vec=vec, top_k=top_k)
        return [r for r in rows if r.get("source") == "audit_log"]

    # ─────────────────────────────────────────────────────────────────────────
    # 血缘检索（接受预算向量，避免内部重复 Embedding）
    # ─────────────────────────────────────────────────────────────────────────
    def _get_lineage_manager(self) -> LineageManager:
        """返回进程级单例，首次调用时构建，后续复用。"""
        return get_or_build_lineage(self._biz, self._vec)

    def _extract_table_names(self, rows: List[Dict[str, Any]]) -> List[str]:
        names: List[str] = []
        for row in rows:
            table_names = row.get("table_names")
            if table_names:
                for item in str(table_names).split(","):
                    name = item.strip().lower()
                    if name:
                        names.append(name)
            content = row.get("content", "")
            match = re.search(r"\bCREATE\s+TABLE\s+`?(\w+(?:\.\w+)?)`?", content, re.IGNORECASE)
            if match:
                names.append(match.group(1).split(".")[-1].lower())
        return names

    def retrieve_lineage(
        self,
        query: str,
        *,
        vec: Optional[List[float]] = None,   # 接受外部预算向量，不再内部重复调 Embedding
        top_k: int | None = None,
        depth: int = 2,
    ) -> List[Dict[str, Any]]:
        manager = self._get_lineage_manager()
        limit = top_k or self._top_k

        # 用 search() 统一接口（vec 有则向量，无则关键词），复用已有召回结果
        ddl_rows = self.search(query, vec=vec, content_type="ddl", top_k=limit)
        sql_rows = self.search(query, vec=vec, content_type="sql", top_k=limit)

        candidate_tables: List[str] = []
        for name in self._extract_table_names(ddl_rows + sql_rows):
            if name not in candidate_tables:
                candidate_tables.append(name)

        if not candidate_tables:
            all_tables = sorted(manager.graph.all_tables())
            lowered = query.lower()
            candidate_tables = [t for t in all_tables if t in lowered][:limit]

        results = []
        for table_name in candidate_tables[:limit]:
            upstream = sorted(manager.graph.get_upstream_tables(table_name, depth=depth))
            downstream = sorted(manager.graph.get_downstream_tables(table_name, depth=depth))
            results.append({
                "table_name": table_name,
                "upstream_tables": upstream,
                "downstream_tables": downstream,
                "summary": (
                    f"核心表 {table_name}；"
                    f"上游 {len(upstream)} 张: {', '.join(upstream) or '无'}；"
                    f"下游 {len(downstream)} 张: {', '.join(downstream) or '无'}"
                ),
            })
        return results
