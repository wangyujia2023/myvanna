"""
audit_log 模式召回 skill。
"""
from __future__ import annotations

from typing import Any, Dict

from .base import BaseSkill, SkillContext
from ..retrieval.doris_knowledge_retriever import DorisRetriever


class AuditPatternSkill(BaseSkill):
    name = "audit_pattern_skill"

    def __init__(self, retriever: DorisRetriever, *, top_k: int = 3):
        self._retriever = retriever
        self._top_k = top_k

    def run(self, context: SkillContext) -> Dict[str, Any]:
        rows = self._retriever.retrieve_audit_patterns(
            context.normalized_query or context.question,
            vec=context.embedding,
            top_k=self._top_k,
        )
        return {"items": rows, "count": len(rows)}
