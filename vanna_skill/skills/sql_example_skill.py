"""
SQL 示例召回 skill。
"""
from __future__ import annotations

from typing import Any, Dict

from .base import BaseSkill, SkillContext
from ..retrieval.doris_retriever import DorisRetriever


class SQLExampleSkill(BaseSkill):
    name = "sql_example_skill"

    def __init__(self, retriever: DorisRetriever, *, top_k: int = 5):
        self._retriever = retriever
        self._top_k = top_k

    def run(self, context: SkillContext) -> Dict[str, Any]:
        rows = self._retriever.retrieve_sql_examples(
            context.normalized_query or context.question,
            vec=context.embedding,
            top_k=self._top_k,
        )
        return {"items": rows, "count": len(rows)}
