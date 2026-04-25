"""
基于 Doris 向量库的多路召回器。
"""
from __future__ import annotations

import json
from typing import Any, Dict, List

from ..doris_client import DorisClient
from ..qwen_client import QwenClient


class DorisRetriever:
    def __init__(
        self,
        vec_client: DorisClient,
        embed_client: QwenClient,
        *,
        db_name: str,
        top_k: int = 5,
        max_distance: float = 1.0,
    ):
        self._vec = vec_client
        self._embed = embed_client
        self._db_name = db_name
        self._top_k = top_k
        self._max_distance = max_distance

    def _search(self, query: str, *, content_type: str, top_k: int | None = None) -> List[Dict[str, Any]]:
        vec = self._embed.get_embedding(query)
        limit = top_k or self._top_k
        rows = self._vec.execute(
            f"""
            SELECT id, content_type, question, content, source, db_name, table_names,
                   quality_score, use_count,
                   cosine_distance(embedding, {json.dumps(vec)}) AS dist
            FROM vanna_store.vanna_embeddings
            WHERE content_type = %s
              AND (db_name = %s OR IFNULL(db_name, '') = '')
            ORDER BY dist ASC
            LIMIT {limit}
            """,
            (content_type, self._db_name),
        )
        return [
            row for row in rows
            if row.get("dist") is not None and row["dist"] < self._max_distance
        ]

    def retrieve_sql_examples(self, query: str, *, top_k: int | None = None) -> List[Dict[str, Any]]:
        return self._search(query, content_type="sql", top_k=top_k)

    def retrieve_ddl(self, query: str, *, top_k: int | None = None) -> List[Dict[str, Any]]:
        return self._search(query, content_type="ddl", top_k=top_k)

    def retrieve_docs(self, query: str, *, top_k: int | None = None) -> List[Dict[str, Any]]:
        return self._search(query, content_type="doc", top_k=top_k)

    def retrieve_audit_patterns(self, query: str, *, top_k: int | None = None) -> List[Dict[str, Any]]:
        rows = self.retrieve_sql_examples(query, top_k=top_k)
        return [row for row in rows if row.get("source") == "audit_log"]
