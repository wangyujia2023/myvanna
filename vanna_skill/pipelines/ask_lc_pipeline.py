"""
LangChain 可扩展 pipeline。
"""
from __future__ import annotations

import json
import logging
from typing import Callable, Dict, Optional

from ..agents.recall_fusion_agent import RecallFusionAgent
from ..agents.router_agent import RouterAgent
from ..agents.sql_generator_agent import SQLGeneratorAgent
from ..agents.sql_guard_agent import SQLGuardAgent
from ..doris_client import DorisClient
from ..qwen_client import QwenClient
from ..retrieval.doris_retriever import DorisRetriever
from ..skills.audit_pattern_skill import AuditPatternSkill
from ..skills.base import SkillContext
from ..skills.business_doc_skill import BusinessDocSkill
from ..skills.doris_schema_skill import DorisSchemaSkill
from ..skills.intent_parse_skill import IntentParseSkill
from ..skills.sql_example_skill import SQLExampleSkill
from ..tracer import RequestTrace, tracer

logger = logging.getLogger(__name__)


class AskLCPipeline:
    def __init__(self, config: dict):
        self._config = config
        self._vec = DorisClient(
            host=config["host"],
            port=config["port"],
            user=config["user"],
            password=config.get("password", ""),
            database="vanna_store",
        )
        self._biz = DorisClient(
            host=config["host"],
            port=config["port"],
            user=config["user"],
            password=config.get("password", ""),
            database=config.get("database", "retail_dw"),
        )
        self._llm = QwenClient(
            api_key=config["qwen_api_key"],
            model=config.get("model", "qwen-plus"),
            embedding_model=config.get("embedding_model", "text-embedding-v3"),
        )
        self._retriever = DorisRetriever(
            self._vec,
            self._llm,
            db_name=config.get("database", "retail_dw"),
            top_k=config.get("n_results", 5),
        )
        self._router = RouterAgent(IntentParseSkill(self._llm))
        self._fusion = RecallFusionAgent([
            DorisSchemaSkill(self._retriever),
            SQLExampleSkill(self._retriever),
            BusinessDocSkill(self._retriever),
            AuditPatternSkill(self._retriever),
        ])
        self._generator = SQLGeneratorAgent(config)
        self._guard = SQLGuardAgent(self._biz)

    def _persist_trace(self, trace: RequestTrace):
        try:
            self._vec.execute_write(
                """
                INSERT INTO vanna_store.vanna_trace_log
                    (trace_id, question, final_sql, status, model_used,
                     total_ms, error_msg, steps_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    trace.trace_id,
                    trace.question,
                    trace.final_sql,
                    trace.status,
                    trace.model_used,
                    round(trace.total_ms, 1),
                    trace.error,
                    json.dumps([s.to_dict() for s in trace.steps], ensure_ascii=False),
                ),
            )
        except Exception as e:
            logger.warning(f"[AskLCPipeline] trace 持久化失败: {e}")

    def run(self, question: str) -> Dict[str, object]:
        return self.run_with_trace(question)

    def run_with_trace(
        self,
        question: str,
        step_callback: Optional[Callable[[str, dict], None]] = None,
    ) -> Dict[str, object]:
        trace = tracer.start(question)
        trace.model_used = self._config.get("model", "qwen-plus")

        def notify(event_type: str, data: dict):
            if step_callback:
                try:
                    step_callback(event_type, data)
                except Exception:
                    pass

        notify("start", {"trace_id": trace.trace_id, "question": question})

        try:
            notify("step_start", {"name": "router_intent", "label": "① 意图解析与标准化"})
            step_router = trace.begin_step("router_intent", {"question": question})
            routed = self._router.run(question)
            step_router.finish(outputs=routed, note=routed.get("intent", ""))
            notify("step_done", step_router.to_dict())

            context = SkillContext(
                question=question,
                normalized_query=routed["normalized_query"],
                intent=routed["intent"],
                entity=routed["entity"],
            )

            notify("step_start", {"name": "multi_recall", "label": "② 多路召回与融合"})
            step_recall = trace.begin_step(
                "multi_recall",
                {
                    "normalized_query": context.normalized_query,
                    "intent": context.intent,
                },
            )
            recall = self._fusion.run(context)
            fused = recall["fused_context"]
            step_recall.finish(outputs={
                "sql_example_count": len(fused.get("sql_examples", [])),
                "ddl_count": len(fused.get("ddl_items", [])),
                "doc_count": len(fused.get("doc_items", [])),
                "audit_count": len(fused.get("audit_items", [])),
                "skill_outputs": recall["skill_outputs"],
            })
            notify("step_done", step_recall.to_dict())

            notify("step_start", {"name": "build_prompt", "label": "③ 组装 Prompt"})
            step_prompt = trace.begin_step("build_prompt")
            generated = self._generator.run(
                question=question,
                normalized_query=context.normalized_query,
                intent=context.intent,
                fused_context=fused,
            )
            payload = generated["prompt_payload"]
            step_prompt.finish(outputs={
                "prompt_len": len(str(payload)),
                "sim_sql_count": len(fused.get("sql_examples", [])),
                "ddl_count": len(fused.get("ddl_items", [])),
                "doc_count": len(fused.get("doc_items", [])),
                "prompt_full": json.dumps(payload, ensure_ascii=False, indent=2),
                "question_sql_examples": [
                    {
                        "question": item.get("question", ""),
                        "sql": item.get("content", ""),
                    }
                    for item in fused.get("sql_examples", [])
                ],
                "ddl_list": [item.get("content", "") for item in fused.get("ddl_items", [])],
                "doc_list": [item.get("content", "") for item in fused.get("doc_items", [])],
            })
            notify("step_done", step_prompt.to_dict())

            notify("step_start", {"name": "llm_generate", "label": "④ LLM 推理生成 SQL"})
            step_llm = trace.begin_step("llm_generate", {"model": trace.model_used})
            sql = generated["sql"]
            step_llm.finish(
                outputs={"response_len": len(sql), "preview": sql[:300]},
                note=trace.model_used,
            )
            notify("step_done", step_llm.to_dict())

            notify("step_start", {"name": "sql_guard", "label": "⑤ SQL Guard / EXPLAIN"})
            step_guard = trace.begin_step("sql_guard")
            guard = self._guard.run(sql)
            step_guard.finish(
                status="ok" if guard.get("ok") else "error",
                outputs=guard,
                error="" if guard.get("ok") else str(guard.get("reason", "")),
            )
            notify("step_done", step_guard.to_dict())

            trace.finish(sql=sql, error="" if guard.get("ok") else str(guard.get("reason", "")))
            self._persist_trace(trace)
            result = {
                "question": question,
                "normalized_query": context.normalized_query,
                "intent": context.intent,
                "entity": context.entity,
                "sql": sql,
                "guard": guard,
                "recall": recall,
                "prompt_payload": payload,
                "trace": trace.to_dict(),
                "error": "" if guard.get("ok") else str(guard.get("reason", "")),
            }
            notify("final", result)
            return result
        except Exception as e:
            trace.finish(error=str(e))
            self._persist_trace(trace)
            logger.exception(f"[AskLCPipeline] 失败: {e}")
            result = {"sql": "", "trace": trace.to_dict(), "error": str(e)}
            notify("error", result)
            return result
