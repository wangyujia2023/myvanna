"""
DDL 召回 skill。
"""
from __future__ import annotations

from typing import Any, Dict

from .base import BaseSkill, SkillContext
from ..retrieval.doris_knowledge_retriever import DorisRetriever


class DorisSchemaSkill(BaseSkill):
    name = "doris_schema_skill"

    def __init__(self, retriever: DorisRetriever, *, top_k: int = 5):
        self._retriever = retriever
        self._top_k = top_k

    def run(self, context: SkillContext) -> Dict[str, Any]:
        rows = self._retriever.retrieve_ddl(
            context.normalized_query or context.question,
            vec=context.embedding,   # None 时自动降级关键词检索
            top_k=self._top_k,
        )
        return {"items": rows, "count": len(rows)}
