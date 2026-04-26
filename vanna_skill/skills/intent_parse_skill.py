"""
意图解析 skill。
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict

logger = logging.getLogger(__name__)

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

    # 在生成前用正则快速识别明显的破坏性关键词，避免浪费一次 LLM 调用
    # Python 3 的 \w 默认包含 Unicode（汉字也是 \w），导致 \b 在中英文衔接处失效。
    # 解决方案：英文关键词用 (?<![a-zA-Z]) / (?![a-zA-Z]) 做纯 ASCII 词边界。
    _DDL_PATTERN = re.compile(
        r"(?:"
        # ── 中文破坏性关键词（直接匹配）
        r"删(?:除|掉|库|表|数据|字段|列|行)|清空|清除数据|改表|改字段|加字段|删字段"
        r"|建表|建库|新建表|修改表|修改字段|截断|丢弃"
        r"|"
        # ── 英文 SQL DDL/DML 关键词（ASCII 标识符边界：前后不能是字母或下划线）
        r"(?<![a-zA-Z_])(?:drop|alter|truncate|delete|update|insert"
        r"|create\s+table|grant|revoke|replace\s+into)(?![a-zA-Z_])"
        r")",
        re.IGNORECASE,
    )

    def run(self, context: SkillContext) -> Dict[str, Any]:
        # ── 快速路径：关键词命中直接返回 invalid，无需 LLM ──────────────────
        if self._DDL_PATTERN.search(context.question):
            logger.info(
                f"[IntentParseSkill] 快速拦截破坏性意图: {context.question[:60]!r}"
            )
            return IntentParseResult(
                query=context.question,
                intent="invalid",
                entity="",
            ).model_dump()

        prompt = (
            "请将用户输入解析成 JSON，字段必须是 query、intent、entity。\n"
            "intent 取值规则：\n"
            "  - data_query      ：需要查询/统计/分析数据\n"
            "  - metric_explain  ：解释指标含义\n"
            "  - root_cause      ：排查数据异常原因\n"
            "  - schema_lookup   ：询问表结构/字段含义\n"
            "  - invalid         ：任何写操作（删表、删数据、修改、插入、建表、授权等）"
            "或与数据查询完全无关的请求\n"
            "只返回 JSON，不要解释。\n\n"
            f"用户输入：{context.question}"
        )
        raw = self._llm.generate(prompt, temperature=0.0).strip()
        # 剥离 LLM 可能包裹的 markdown 代码块（```json ... ``` 或 ``` ... ```）
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw).strip()
        try:
            data = json.loads(raw)
            parsed = IntentParseResult(**data)
        except Exception as exc:
            logger.warning(
                f"[IntentParseSkill] JSON 解析失败，降级 data_query。"
                f" raw={raw[:120]!r}  err={exc}"
            )
            parsed = IntentParseResult(
                query=context.question,
                intent="data_query",
                entity="",
            )
        return parsed.model_dump()
