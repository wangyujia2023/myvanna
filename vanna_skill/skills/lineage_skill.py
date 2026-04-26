"""
血缘召回 skill。
"""
from __future__ import annotations

from typing import Dict

from .base import BaseSkill, SkillContext
from ..retrieval.doris_retriever import DorisRetriever


class LineageSkill(BaseSkill):
    name = "lineage_skill"

    def __init__(self, retriever: DorisRetriever, *, top_k: int = 3, depth: int = 2):
        self._retriever = retriever
        self._top_k = top_k
        self._depth = depth

    def run(self, context: SkillContext) -> Dict[str, object]:
        rows = self._retriever.retrieve_lineage(
            context.normalized_query or context.question,
            vec=context.embedding,   # 复用预算向量，不再内部重复调 Embedding
            top_k=self._top_k,
            depth=self._depth,
        )
        return {
            "items": rows,
            "count": len(rows),
            "tables": [item.get("table_name", "") for item in rows],
        }
