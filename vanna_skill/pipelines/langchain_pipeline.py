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
from ..retrieval.doris_knowledge_retriever import DorisRetriever
from ..skills.audit_pattern_skill import AuditPatternSkill
from ..skills.base import SkillContext
from ..skills.business_doc_skill import BusinessDocSkill
from ..skills.doris_schema_skill import DorisSchemaSkill
from ..skills.intent_parse_skill import IntentParseSkill
from ..skills.lineage_skill import LineageSkill
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
            biz_client=self._biz,
            top_k=config.get("n_results", 5),
        )
        self._router = RouterAgent(IntentParseSkill(self._llm))
        self._fusion = RecallFusionAgent([
            DorisSchemaSkill(self._retriever),
            SQLExampleSkill(self._retriever),
            BusinessDocSkill(self._retriever),
            AuditPatternSkill(self._retriever),
            LineageSkill(self._retriever),
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

    def run(self, question: str, prompt_version: Optional[str] = None) -> Dict[str, object]:
        return self.run_with_trace(question, prompt_version=prompt_version)

    def run_with_trace(
        self,
        question: str,
        prompt_version: Optional[str] = None,
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

            # ── 意图守卫：非查询意图直接拒绝，不进入后续 pipeline ────────────
            if routed.get("intent") == "invalid":
                rejection = (
                    "该请求涉及写操作或与数据查询无关，系统只支持 SELECT 查询，"
                    "无法执行删除、修改、建表等操作。"
                )
                trace.finish(sql="", error=rejection)
                self._persist_trace(trace)
                result = {
                    "question": question,
                    "normalized_query": routed.get("normalized_query", question),
                    "intent": "invalid",
                    "entity": routed.get("entity", ""),
                    "sql": "",
                    "guard": {"ok": False, "reason": rejection},
                    "error": rejection,
                    "trace": trace.to_dict(),
                }
                notify("final", result)
                return result

            # ── Embedding 前置：整个 pipeline 只算一次向量 ────────────────────
            # embedding_fallback_mode:
            #   "fail"    → Embedding 失败直接抛出，pipeline 终止
            #   "keyword" → 降级为关键词检索，context.embedding 置 None（默认）
            query_text = routed["normalized_query"] or question
            fallback_enabled = bool(self._config.get("langchain_fallback_enabled", False))
            fallback_mode = "keyword" if fallback_enabled else "fail"
            logger.info(
                "[AskLCPipeline] embedding fallback enabled=%s mode=%s query=%s",
                fallback_enabled,
                fallback_mode,
                query_text[:120],
            )
            try:
                query_vec = self._llm.get_embedding(query_text)
                logger.info("[AskLCPipeline] Embedding OK dims=%s", len(query_vec))
            except Exception as emb_err:
                if fallback_mode == "fail":
                    logger.error(
                        "[AskLCPipeline] Embedding 失败且禁止降级，query=%s err=%s",
                        query_text[:120],
                        emb_err,
                    )
                    raise RuntimeError(
                        f"Embedding 计算失败，pipeline 终止（embedding_fallback_mode=fail）: {emb_err}"
                    ) from emb_err
                logger.warning(
                    "[AskLCPipeline] Embedding 失败，降级关键词检索 query=%s err=%s",
                    query_text[:120],
                    emb_err,
                )
                query_vec = None   # Skills 收到 None → 自动走 _search_by_keyword

            context = SkillContext(
                question=question,
                normalized_query=routed["normalized_query"],
                intent=routed["intent"],
                entity=routed["entity"],
                embedding=query_vec,   # 预算向量挂在 context，所有 Skill 复用
            )

            notify("step_start", {"name": "multi_recall", "label": "② 多路召回与融合"})
            step_recall = trace.begin_step(
                "multi_recall",
                {
                    "normalized_query": context.normalized_query,
                    "intent": context.intent,
                    "embedding_mode": "vector" if query_vec is not None else f"keyword({fallback_mode})",
                    "fallback_enabled": fallback_enabled,
                },
            )
            recall = self._fusion.run(context)
            fused = recall["fused_context"]
            step_recall.finish(outputs={
                "sql_example_count": len(fused.get("sql_examples", [])),
                "ddl_count": len(fused.get("ddl_items", [])),
                "doc_count": len(fused.get("doc_items", [])),
                "audit_count": len(fused.get("audit_items", [])),
                "lineage_count": len(fused.get("lineage_items", [])),
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
                prompt_version=prompt_version,
            )
            payload = generated["prompt_payload"]
            step_prompt.finish(outputs={
                "prompt_len": len(str(payload)),
                "sim_sql_count": len(fused.get("sql_examples", [])),
                "ddl_count": len(fused.get("ddl_items", [])),
                "doc_count": len(fused.get("doc_items", [])),
                "lineage_count": len(fused.get("lineage_items", [])),
                "prompt_version": generated.get("prompt_version", "default"),
                "prompt_name": generated.get("prompt_name", "Default"),
                "prompt_full": json.dumps(payload, ensure_ascii=False, indent=2),
                "question_sql_examples": [
                    {
                        "question": item.get("question", ""),
                        "sql": item.get("content", ""),
                        "score": round(max(0.0, 1.0 - float(item.get("dist", 0) or 0)), 4) if item.get("dist") is not None else None,
                        "distance": round(float(item.get("dist", 0) or 0), 4) if item.get("dist") is not None else None,
                        "quality": round(float(item.get("quality_score", 0) or 0), 4),
                    }
                    for item in fused.get("sql_examples", [])
                ],
                "ddl_list": [item.get("content", "") for item in fused.get("ddl_items", [])],
                "doc_list": [item.get("content", "") for item in fused.get("doc_items", [])],
                "lineage_list": fused.get("lineage_items", []),
            })
            notify("step_done", step_prompt.to_dict())

            # ── ④⑤ LLM 生成 + SQL Guard + 自修正循环（最多3次）──────────────
            MAX_ATTEMPTS = 3
            sql = generated["sql"]
            guard: dict = {}
            correction_hint = ""   # 上次失败原因，回传给 LLM 修正

            for attempt in range(1, MAX_ATTEMPTS + 1):
                label_gen = f"④ LLM 推理生成 SQL" + (f"（第{attempt}次）" if attempt > 1 else "")
                notify("step_start", {"name": f"llm_generate_{attempt}", "label": label_gen})
                step_llm = trace.begin_step(
                    f"llm_generate_{attempt}",
                    {"model": trace.model_used, "attempt": attempt},
                )

                if attempt == 1:
                    # 第一次直接使用已生成的 SQL
                    pass
                else:
                    # 后续重试：把上次错误作为修正提示注入 fused_context
                    try:
                        fused_with_hint = dict(fused)
                        fused_with_hint["correction_hint"] = correction_hint
                        re_generated = self._generator.run(
                            question=question,
                            normalized_query=context.normalized_query,
                            intent=context.intent,
                            fused_context=fused_with_hint,
                            prompt_version=prompt_version,
                        )
                        sql = re_generated["sql"]
                        payload = re_generated["prompt_payload"]
                    except ValueError as gen_err:
                        # 生成阶段危险词拦截
                        step_llm.finish(status="error", error=str(gen_err))
                        notify("step_done", step_llm.to_dict())
                        raise

                step_llm.finish(
                    outputs={"response_len": len(sql), "preview": sql[:300], "attempt": attempt},
                    note=f"{trace.model_used} attempt={attempt}",
                )
                notify("step_done", step_llm.to_dict())

                label_guard = f"⑤ SQL Guard / EXPLAIN" + (f"（第{attempt}次）" if attempt > 1 else "")
                notify("step_start", {"name": f"sql_guard_{attempt}", "label": label_guard})
                step_guard = trace.begin_step(f"sql_guard_{attempt}", {"attempt": attempt})
                guard = self._guard.run(sql)
                step_guard.finish(
                    status="ok" if guard.get("ok") else "error",
                    outputs=guard,
                    error="" if guard.get("ok") else str(guard.get("reason", "")),
                )
                notify("step_done", step_guard.to_dict())

                if guard.get("ok"):
                    break

                # Guard 失败：准备下一轮修正提示
                reason = guard.get("reason", "未知错误")
                correction_hint = (
                    f"上一次生成的 SQL 存在错误，错误信息：{reason}。\n"
                    f"错误 SQL：{sql}\n"
                    f"请根据错误信息修正 SQL，只输出修正后的 SQL。"
                )
                logger.warning(
                    f"[AskLCPipeline] SQL Guard 失败 attempt={attempt}: {reason}"
                )
                if attempt == MAX_ATTEMPTS:
                    logger.error(f"[AskLCPipeline] 自修正耗尽 {MAX_ATTEMPTS} 次，最终失败")

            trace.finish(sql=sql, error="" if guard.get("ok") else str(guard.get("reason", "")))
            self._persist_trace(trace)
            result = {
                "question": question,
                "normalized_query": context.normalized_query,
                "intent": context.intent,
                "entity": context.entity,
                "sql": sql,
                "prompt_version": generated.get("prompt_version", "default"),
                "prompt_name": generated.get("prompt_name", "Default"),
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
