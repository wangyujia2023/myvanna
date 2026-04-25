"""
意图解析 skill。
"""
from __future__ import annotations

import json
from typing import Any, Dict

from pydantic import BaseModel, Field

from .base import BaseSkill, SkillContext
from ..qwen_client import QwenClient


class IntentParseResult(BaseModel):
    query: str = Field(..., description="标准化后的查询文本")
    intent: str = Field(..., description="任务意图，如 data_query / metric_explain / root_cause / schema_lookup / invalid")
    entity: str = Field(default="", description="核心业务实体或指标")


class IntentParseSkill(BaseSkill):
    name = "intent_parse_skill"

    def __init__(self, llm: QwenClient):
        self._llm = llm

    def run(self, context: SkillContext) -> Dict[str, Any]:
        prompt = (
            "请将用户输入解析成 JSON，字段必须是 query、intent、entity。\n"
            "intent 只能是 data_query、metric_explain、root_cause、schema_lookup、invalid 之一。\n"
            "只返回 JSON，不要解释。\n\n"
            f"用户输入：{context.question}"
        )
        raw = self._llm.generate(prompt, temperature=0.0).strip()
        try:
            data = json.loads(raw)
            parsed = IntentParseResult(**data)
        except Exception:
            parsed = IntentParseResult(
                query=context.question,
                intent="data_query",
                entity="",
            )
        return parsed.model_dump()
