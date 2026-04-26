"""
IntentUnderstandingAgent：意图理解 Agent（语义层版）

职责：
  1. 快速 DDL/DML 拦截（正则，无 LLM 开销）
  2. 调用 LLM 将自然语言解析成结构化 IntentPlan
  3. 输出业务域 (business_domain)、复杂度 (complexity)、时间提示、实体提示

与旧版 IntentParseSkill 的区别：
  - 输出 IntentPlan dataclass，而非简单的 {query, intent, entity} dict
  - 感知 SemanticCatalog 的业务域（business_summary 注入提示）
  - complexity 字段支持 attribution（P2 预留）
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ..semantic.models import IntentPlan

if TYPE_CHECKING:
    from ..semantic.catalog import SemanticCatalog
    from ..qwen_client import QwenClient

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# 正则：破坏性关键词快速拦截（与 intent_parse_skill 保持一致）
# ─────────────────────────────────────────────────────────────────────────────
_DDL_PATTERN = re.compile(
    r"(?:"
    r"删(?:除|掉|库|表|数据|字段|列|行)|清空|清除数据|改表|改字段|加字段|删字段"
    r"|建表|建库|新建表|修改表|修改字段|截断|丢弃"
    r"|"
    r"(?<![a-zA-Z_])(?:drop|alter|truncate|delete|update|insert"
    r"|create\s+table|grant|revoke|replace\s+into)(?![a-zA-Z_])"
    r")",
    re.IGNORECASE,
)

# 正则：时间提示识别（从用户问句中提取，供 SemanticParseAgent 使用）
_TIME_PATTERNS: List[tuple] = [
    # 今年4月 / 本年4月份 / 去年4月
    (re.compile(r"(今年|本年|去年)\s*(\d{1,2})\s*月"), "relative_year_month"),
    # 4月 / 4月份（默认按当前年份理解）
    (re.compile(r"(?<!\d)(\d{1,2})\s*月(?:份)?"), "month_only"),
    # 具体月份 2026-04 / 2026年4月
    (re.compile(r"(\d{4})[-年](\d{1,2})(?:月|$)"), "month"),
    # 具体年份 2026年
    (re.compile(r"(\d{4})年"), "year"),
    # 最近N天
    (re.compile(r"最近\s*(\d+)\s*天"), "recent_days"),
    # 本月/本年/上月/上季度
    (re.compile(r"本月|当月"), "this_month"),
    (re.compile(r"本年|今年"), "this_year"),
    (re.compile(r"上月|上个月"), "last_month"),
    (re.compile(r"上季度"), "last_quarter"),
    (re.compile(r"本季度|当季"), "this_quarter"),
]


class IntentUnderstandingAgent:
    """
    意图理解 Agent，输出 IntentPlan。

    Parameters
    ----------
    llm      : QwenClient 或兼容接口
    catalog  : SemanticCatalog，可选；提供时注入业务域信息
    """

    def __init__(
        self,
        llm: "QwenClient",
        catalog: Optional["SemanticCatalog"] = None,
    ) -> None:
        self._llm = llm
        self._catalog = catalog

    # ─────────────────────────────────────────────────────────────────────────
    # 公共入口
    # ─────────────────────────────────────────────────────────────────────────

    def run(self, question: str) -> IntentPlan:
        """
        解析用户问题，返回 IntentPlan。
        所有异常内部消化，最差情况返回 data_query 降级计划。
        """
        question = question.strip()

        # 1. 快速拦截破坏性意图
        if _DDL_PATTERN.search(question):
            logger.info(f"[IntentAgent] DDL拦截: {question[:60]!r}")
            return IntentPlan(
                intent_type="invalid",
                business_domain="unknown",
                complexity="simple",
                raw_question=question,
                normalized_query=question,
                rejection_reason="请求包含写操作或破坏性指令，系统仅支持 SELECT 查询。",
            )

        # 2. 提取时间提示（本地，不走 LLM）
        time_hint = self._extract_time_hint(question)

        # 3. 构建 LLM 提示并解析
        plan = self._llm_parse(question, time_hint)
        return plan

    # ─────────────────────────────────────────────────────────────────────────
    # 内部：时间提示提取
    # ─────────────────────────────────────────────────────────────────────────

    def _extract_time_hint(self, question: str) -> str:
        """从问句中提取时间提示字符串，如 '2026-04' / '最近30天'。"""
        now = datetime.now()
        for pattern, hint_type in _TIME_PATTERNS:
            m = pattern.search(question)
            if m:
                if hint_type == "relative_year_month":
                    base_year = now.year
                    if m.group(1) == "去年":
                        base_year -= 1
                    return f"{base_year}-{int(m.group(2)):02d}"
                if hint_type == "month_only":
                    return f"{now.year}-{int(m.group(1)):02d}"
                if hint_type == "month":
                    return f"{m.group(1)}-{int(m.group(2)):02d}"
                if hint_type == "year":
                    return m.group(1)
                if hint_type == "recent_days":
                    return f"最近{m.group(1)}天"
                # 其他类型：返回匹配文本
                return m.group(0)
        return ""

    # ─────────────────────────────────────────────────────────────────────────
    # 内部：LLM 解析
    # ─────────────────────────────────────────────────────────────────────────

    def _llm_parse(self, question: str, time_hint: str) -> IntentPlan:
        """调用 LLM 输出结构化意图，解析失败时返回降级计划。"""
        business_context = self._build_business_context()
        prompt = self._build_prompt(question, time_hint, business_context)

        try:
            raw = self._llm.generate(prompt, temperature=0.0).strip()
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw).strip()
            data: Dict[str, Any] = json.loads(raw)
        except Exception as exc:
            logger.warning(f"[IntentAgent] LLM/JSON 失败，降级: {exc}")
            return self._fallback_plan(question, time_hint)

        return self._build_plan(question, time_hint, data)

    def _build_business_context(self) -> str:
        """把 SemanticCatalog 的业务域摘要注入提示。"""
        if self._catalog is None:
            return ""
        try:
            summary = self._catalog.business_summary()
            if summary:
                return f"\n\n已知业务域（business_domain 必须是下列之一或 unknown）：\n{summary}"
        except Exception:
            pass
        return ""

    def _build_prompt(
        self, question: str, time_hint: str, business_context: str
    ) -> str:
        time_note = f"\n已识别到时间提示：{time_hint}" if time_hint else ""
        return (
            "你是一个数据分析意图理解模型。请将用户问题解析为 JSON，"
            "字段说明如下：\n"
            "  intent_type     : data_query | metric_explain | root_cause | schema_lookup | attribution | invalid\n"
            "  business_domain : 业务域名称（英文），如 sales_overview / user_analysis / unknown\n"
            "  complexity      : simple | compound | attribution\n"
            "  normalized_query: 标准化后的问句（去除口语化、补全缺失主语）\n"
            "  entity_hints    : 涉及的实体中文名列表，如 [\"门店\",\"商品\"]\n"
            "  action_hints    : 涉及的分析动作，如 [\"排名\",\"对比\",\"趋势\"]\n"
            "只返回 JSON，不要解释。"
            f"{business_context}"
            f"{time_note}\n\n"
            f"用户问题：{question}"
        )

    def _build_plan(
        self, question: str, time_hint: str, data: Dict[str, Any]
    ) -> IntentPlan:
        """从 LLM 返回的 dict 构建 IntentPlan，字段做安全转换。"""
        intent_type = str(data.get("intent_type", "data_query"))
        # 合法性校验
        valid_intents = {"data_query", "metric_explain", "root_cause",
                         "schema_lookup", "attribution", "invalid"}
        if intent_type not in valid_intents:
            intent_type = "data_query"

        business_domain = str(data.get("business_domain", "unknown")).strip() or "unknown"
        complexity = str(data.get("complexity", "simple"))
        if complexity not in ("simple", "compound", "attribution"):
            complexity = "simple"

        entity_hints = data.get("entity_hints", [])
        if not isinstance(entity_hints, list):
            entity_hints = []

        action_hints = data.get("action_hints", [])
        if not isinstance(action_hints, list):
            action_hints = []

        normalized = str(data.get("normalized_query", question)).strip() or question

        return IntentPlan(
            intent_type=intent_type,
            business_domain=business_domain,
            complexity=complexity,
            time_hint=time_hint,
            entity_hints=entity_hints,
            action_hints=action_hints,
            raw_question=question,
            normalized_query=normalized,
        )

    def _fallback_plan(self, question: str, time_hint: str) -> IntentPlan:
        return IntentPlan(
            intent_type="data_query",
            business_domain="unknown",
            complexity="simple",
            time_hint=time_hint,
            raw_question=question,
            normalized_query=question,
        )
