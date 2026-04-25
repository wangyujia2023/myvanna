"""
SQL 生成 agent：使用 LangChain + Qwen 兼容接口。
"""
from __future__ import annotations

from typing import Dict, List

from langchain_core.output_parsers import StrOutputParser
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langsmith import Client
from langsmith.run_helpers import tracing_context

from .base import BaseAgent


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
                    "你是 Doris SQL 生成 Agent。只输出可执行 SQL，不要解释，不要 markdown 代码块。",
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
                    "请生成 Doris SQL。",
                ),
            ])
            | self._llm
            | StrOutputParser()
        )

    def run(self, *, question: str, normalized_query: str, intent: str, fused_context: Dict[str, object]) -> Dict[str, object]:
        payload = {
            "system_prompt": self._config.get("initial_prompt") or "你是一个严谨的 Doris SQL 生成器。",
            "question": question,
            "normalized_query": normalized_query,
            "intent": intent,
            "ddl_context": _format_contents(fused_context.get("ddl_items", [])),
            "sql_examples": _format_sql_examples(fused_context.get("sql_examples", [])),
            "doc_context": _format_contents(fused_context.get("doc_items", [])),
            "audit_context": _format_sql_examples(fused_context.get("audit_items", [])),
        }
        enabled = bool(self._config.get("langsmith_api_key"))
        with tracing_context(
            enabled=enabled,
            client=self._langsmith_client,
            project_name="myvanna-langchain",
        ):
            sql = self._chain.invoke(payload)
        return {"sql": sql.strip(), "prompt_payload": payload}
