"""
SQL 生成 agent：使用 LangChain + Qwen 兼容接口。
"""
from __future__ import annotations

import logging
import re
from typing import Dict, List

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langsmith import Client
from langsmith.run_helpers import tracing_context

from .base import BaseAgent

logger = logging.getLogger(__name__)

# ── 第一层防御：生成阶段危险词检测 ──────────────────────────────────────────
# SQLGuard 是最终防线；这里是生成阶段的前置拦截，避免危险 SQL 进入自修正循环。
_UNSAFE_PATTERN = re.compile(
    r"\b(drop|alter|truncate|delete|update|insert|create|grant|revoke|replace)\b",
    re.IGNORECASE,
)

# System Prompt 中明确告知 LLM 只输出 SELECT
_SAFETY_INSTRUCTION = (
    "【安全规则】只能生成 SELECT 查询语句。"
    "严禁生成 DROP / ALTER / TRUNCATE / DELETE / UPDATE / INSERT / CREATE / GRANT / REVOKE 等任何写操作语句。"
    "违反此规则的输出视为无效。"
)


def _format_sql_examples(items: List[dict]) -> str:
    if not items:
        return "无"
    blocks = []
    for idx, item in enumerate(items, 1):
        blocks.append(
            f"[示例{idx}] 问题: {item.get('question','')}\nSQL:\n{item.get('content','')}"
        )
    return "\n\n".join(blocks)


def _format_contents(items: List[dict]) -> str:
    if not items:
        return "无"
    return "\n\n".join(item.get("content", "") for item in items)


def _format_lineage(items: List[dict]) -> str:
    if not items:
        return "无"
    blocks = []
    for idx, item in enumerate(items, 1):
        blocks.append(
            f"[血缘{idx}] 核心表: {item.get('table_name','')}\n"
            f"上游: {', '.join(item.get('upstream_tables', [])) or '无'}\n"
            f"下游: {', '.join(item.get('downstream_tables', [])) or '无'}\n"
            f"说明: {item.get('summary', '')}"
        )
    return "\n\n".join(blocks)


class SQLGeneratorAgent(BaseAgent):
    name = "sql_generator_agent"

    def __init__(self, config: dict):
        self._config = config
        self._llm = ChatOpenAI(
            api_key=config["qwen_api_key"],
            base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
            model=config.get("model", "qwen-plus"),
            temperature=0.05,
            max_retries=2,
        )
        self._langsmith_client = (
            Client(api_key=config.get("langsmith_api_key"))
            if config.get("langsmith_api_key")
            else None
        )
        self._chain = (
            ChatPromptTemplate.from_messages([
                (
                    "system",
                    "{system_prompt}\n\n"
                    "你是 Doris SQL 生成 Agent。只输出可执行 SQL，不要解释，不要 markdown 代码块。\n"
                    f"{_SAFETY_INSTRUCTION}",
                ),
                (
                    "human",
                    "用户问题:\n{question}\n\n"
                    "标准化查询:\n{normalized_query}\n\n"
                    "任务意图:\n{intent}\n\n"
                    "DDL 上下文:\n{ddl_context}\n\n"
                    "SQL 示例:\n{sql_examples}\n\n"
                    "业务文档:\n{doc_context}\n\n"
                    "audit 模式:\n{audit_context}\n\n"
                    "血缘上下文:\n{lineage_context}\n\n"
                    "{correction_hint}"
                    "请生成 Doris SQL。",
                ),
            ])
            | self._llm
            | StrOutputParser()
        )

    def _resolve_prompt(self, prompt_version: str | None = None) -> dict:
        versions = self._config.get("prompt_versions") or []
        version_id = prompt_version or self._config.get("active_prompt_version") or "default"
        for item in versions:
            if item.get("id") == version_id:
                return item
        for item in versions:
            if item.get("id") == "default":
                return item
        return {
            "id": "default",
            "name": "Default",
            "description": "",
            "system_prompt": self._config.get("initial_prompt") or "你是一个严谨的 Doris SQL 生成器。",
        }

    def run(
        self,
        *,
        question: str,
        normalized_query: str,
        intent: str,
        fused_context: Dict[str, object],
        prompt_version: str | None = None,
    ) -> Dict[str, object]:
        selected_prompt = self._resolve_prompt(prompt_version)
        # correction_hint 由自修正循环注入（首次为空字符串）
        raw_hint = fused_context.get("correction_hint", "") or ""
        correction_hint_str = (
            f"【修正提示】{raw_hint}\n\n" if raw_hint else ""
        )
        payload = {
            "system_prompt": selected_prompt.get("system_prompt") or "你是一个严谨的 Doris SQL 生成器。",
            "question": question,
            "normalized_query": normalized_query,
            "intent": intent,
            "ddl_context": _format_contents(fused_context.get("ddl_items", [])),
            "sql_examples": _format_sql_examples(fused_context.get("sql_examples", [])),
            "doc_context": _format_contents(fused_context.get("doc_items", [])),
            "audit_context": _format_sql_examples(fused_context.get("audit_items", [])),
            "lineage_context": _format_lineage(fused_context.get("lineage_items", [])),
            "correction_hint": correction_hint_str,
        }
        enabled = bool(self._config.get("langsmith_api_key"))
        with tracing_context(
            enabled=enabled,
            client=self._langsmith_client,
            project_name="myvanna-langchain",
        ):
            sql = self._chain.invoke(payload).strip()

        # ── 第一层防御：生成后立即检测危险词 ────────────────────────────────
        match = _UNSAFE_PATTERN.search(sql)
        if match:
            raise ValueError(
                f"生成 SQL 含危险关键词 '{match.group()}'，已在生成阶段拦截。"
                f" SQL 预览: {sql[:120]}"
            )

        return {
            "sql": sql,
            "prompt_payload": payload,
            "prompt_version": selected_prompt.get("id", "default"),
            "prompt_name": selected_prompt.get("name", "Default"),
        }
