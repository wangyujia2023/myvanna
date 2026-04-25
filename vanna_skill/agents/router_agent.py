"""
路由 agent：负责标准化问题和意图。
"""
from __future__ import annotations

from typing import Dict

from .base import BaseAgent
from ..skills.base import SkillContext
from ..skills.intent_parse_skill import IntentParseSkill


class RouterAgent(BaseAgent):
    name = "router_agent"

    def __init__(self, intent_skill: IntentParseSkill):
        self._intent_skill = intent_skill

    def run(self, question: str) -> Dict[str, object]:
        ctx = SkillContext(question=question)
        parsed = self._intent_skill.run(ctx)
        return {
            "question": question,
            "normalized_query": parsed["query"],
            "intent": parsed["intent"],
            "entity": parsed["entity"],
        }
