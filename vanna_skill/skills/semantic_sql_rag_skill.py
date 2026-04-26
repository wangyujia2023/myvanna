"""
Semantic 独立 SQL RAG skill。
"""
from __future__ import annotations

from typing import Any, Dict

from .base import BaseSkill, SkillContext
from ..semantic.semantic_sql_rag import SemanticSQLRAGStore


class SemanticSQLRAGSkill(BaseSkill):
    name = "semantic_sql_rag_skill"

    def __init__(self, store: SemanticSQLRAGStore, *, top_k: int = 5):
        self._store = store
        self._top_k = top_k

    def run(self, context: SkillContext) -> Dict[str, Any]:
        items = self._store.search(
            context.normalized_query or context.question,
            top_k=self._top_k,
        )
        return {"items": items, "count": len(items)}
