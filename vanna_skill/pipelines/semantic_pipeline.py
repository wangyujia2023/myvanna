"""
SemanticPipeline：语义路径主 Pipeline

架构（5 Agent 链式执行）：
  ① IntentUnderstandingAgent  → IntentPlan
  ② SemanticParseAgent        → SemanticPlan (+ coverage_score)
  ③ QueryPlanAgent            → QueryPlan (DAG)
  ④ SQLGeneratorSemanticAgent → SQL 字符串
  ⑤ LangChain 降级路径        → 当 coverage_score < 0.5 时走 AskLCPipeline

路由规则（基于 coverage_score）：
  ≥ 0.8  → SQLSynthesizer 模板路径（不调 LLM）
  0.5-0.8 → LLM 辅助路径（语义上下文 + LLM 生成）
  < 0.5  → LangChain 降级（AskLCPipeline，SemanticPlan 作为额外 hint）

与旧版 AskLCPipeline 的关系：
  - AskLCPipeline 保留为白名单降级路径
  - 新增 /ask/semantic 入口调用本 Pipeline
  - UI 右上角 toggle 控制使用哪个路径
"""
from __future__ import annotations

import json
import logging
from typing import Callable, Dict, List, Optional

from ..agents.intent_agent import IntentUnderstandingAgent
from ..agents.query_plan_agent import QueryPlanAgent
from ..agents.semantic_parse_agent import SemanticParseAgent
from ..agents.sql_generator_semantic import SQLGeneratorSemanticAgent
from ..agents.sql_guard_agent import SQLGuardAgent
from ..doris_client import DorisClient
from ..qwen_client import QwenClient
from ..semantic.catalog import get_catalog
from ..semantic.models import IntentPlan, QueryPlan, SemanticPlan, SemanticResult
from ..tracer import RequestTrace, tracer

logger = logging.getLogger(__name__)

# coverage_score 阈值
THRESHOLD_FULL_SEMANTIC = 0.8    # 全语义路径
THRESHOLD_HYBRID = 0.5           # 混合路径（LLM 辅助）


class SemanticPipeline:
    """
    语义路径主 Pipeline。

    Parameters
    ----------
    config : 与 AskLCPipeline 相同的配置 dict
    """

    def __init__(self, config: dict) -> None:
        self._config = config
        self._semantic_fallback_enabled = bool(
            config.get("semantic_to_langchain_fallback_enabled", False)
        )

        # ── DB 连接 ────────────────────────────────────────────────────────
        _conn = dict(
            host=config["host"],
            port=config["port"],
            user=config["user"],
            password=config.get("password", ""),
        )
        self._vec = DorisClient(**_conn, database="vanna_store")     # 向量/Trace 库
        self._sem = DorisClient(**_conn, database="semantic_store")  # 语义知识库（独立）
        self._biz = DorisClient(**_conn, database=config.get("database", "retail_dw"))

        # ── LLM ───────────────────────────────────────────────────────────
        self._llm = QwenClient(
            api_key=config["qwen_api_key"],
            model=config.get("model", "qwen-plus"),
            embedding_model=config.get("embedding_model", "text-embedding-v3"),
        )

        # ── SemanticCatalog（进程级单例，使用独立的 semantic_store）────────
        self._catalog = get_catalog(
            self._sem,
            db_name=config.get("database", "retail_dw"),
        )

        # ── Guard ─────────────────────────────────────────────────────────
        self._guard = SQLGuardAgent(self._biz)

        # ── Agents ────────────────────────────────────────────────────────
        self._intent_agent = IntentUnderstandingAgent(self._llm, self._catalog)
        self._semantic_parse = SemanticParseAgent(self._llm, self._catalog)
        self._query_plan = QueryPlanAgent(self._catalog)
        self._sql_gen = SQLGeneratorSemanticAgent(
            self._llm, self._catalog, self._guard
        )

        # ── LangChain 降级路径（延迟初始化，避免循环依赖）─────────────────
        self._lc_pipeline = None

    def _get_lc_pipeline(self):
        """延迟初始化 LangChain 降级 pipeline。"""
        if self._lc_pipeline is None:
            from .ask_lc_pipeline import AskLCPipeline
            self._lc_pipeline = AskLCPipeline(self._config)
        return self._lc_pipeline

    # ─────────────────────────────────────────────────────────────────────────
    # 公共入口
    # ─────────────────────────────────────────────────────────────────────────

    def run(
        self,
        question: str,
        step_callback: Optional[Callable[[str, dict], None]] = None,
    ) -> Dict:
        """
        执行语义路径查询，返回统一格式 dict（兼容前端结构）。
        """
        trace = tracer.start(question)
        trace.model_used = self._config.get("model", "qwen-plus")

        def notify(event: str, data: dict):
            if step_callback:
                try:
                    step_callback(event, data)
                except Exception:
                    pass

        notify("start", {"trace_id": trace.trace_id, "question": question, "path": "semantic"})
        logger.info(
            "[SemanticPipeline] start question=%r semantic_fallback_enabled=%s model=%s",
            question,
            self._semantic_fallback_enabled,
            self._config.get("model", "qwen-plus"),
        )
        self._catalog.refresh_from_db(required=True)

        try:
            result = self._execute(question, trace, notify)
        except Exception as exc:
            logger.exception(f"[SemanticPipeline] 异常: {exc}")
            trace.finish(error=str(exc))
            self._persist_trace(trace)
            result = {"sql": "", "error": str(exc), "trace": trace.to_dict()}
            notify("error", result)

        return result

    # ─────────────────────────────────────────────────────────────────────────
    # 内部：主执行流程
    # ─────────────────────────────────────────────────────────────────────────

    def _execute(
        self,
        question: str,
        trace: RequestTrace,
        notify: Callable,
    ) -> Dict:

        # ── Step 1: 意图理解 ───────────────────────────────────────────────
        notify("step_start", {"name": "intent", "label": "① 意图理解"})
        step = trace.begin_step("intent", {"question": question})
        intent: IntentPlan = self._intent_agent.run(question)
        step.finish(outputs={
            "intent_type": intent.intent_type,
            "business_domain": intent.business_domain,
            "complexity": intent.complexity,
            "time_hint": intent.time_hint,
        })
        notify("step_done", step.to_dict())

        # invalid 意图：立即拒绝
        if intent.intent_type == "invalid":
            rejection = intent.rejection_reason or (
                "该请求涉及写操作或与数据查询无关，系统只支持 SELECT 查询。"
            )
            trace.finish(sql="", error=rejection)
            self._persist_trace(trace)
            result = self._make_result(
                question=question,
                intent=intent,
                sql="",
                path="semantic",
                guard={"ok": False, "reason": rejection},
                error=rejection,
                trace=trace,
            )
            notify("final", result)
            return result

        # non-query 意图：降级到 LangChain（schema_lookup / metric_explain）
        if intent.intent_type not in ("data_query", "attribution", "root_cause"):
            logger.info(
                "[SemanticPipeline] 非数据查询意图=%s，触发 LangChain 降级，semantic_fallback_enabled=%s",
                intent.intent_type,
                self._semantic_fallback_enabled,
            )
            return self._lc_fallback(
                question,
                trace,
                notify,
                intent,
                fallback_reason=f"non_query_intent:{intent.intent_type}",
            )

        # ── Step 2: 语义解析 ───────────────────────────────────────────────
        notify("step_start", {"name": "semantic_parse", "label": "② 语义解析"})
        step = trace.begin_step("semantic_parse", {"normalized_query": intent.normalized_query})
        sp: SemanticPlan = self._semantic_parse.run(intent)
        step.finish(outputs={
            "metrics": sp.metrics,
            "dimensions": sp.dimensions,
            "coverage_score": sp.coverage_score,
            "fallback_threshold": THRESHOLD_HYBRID,
            "semantic_fallback_enabled": self._semantic_fallback_enabled,
            "analysis_type": sp.query_spec.analysis_type if sp.query_spec else "",
            "time_scope": (
                {
                    "start": sp.query_spec.time_scope.start,
                    "end": sp.query_spec.time_scope.end,
                    "label": sp.query_spec.time_scope.label,
                }
                if sp.query_spec and sp.query_spec.time_scope else {}
            ),
            "comparison": (
                {
                    "mode": sp.query_spec.comparison.mode,
                    "compare_start": sp.query_spec.comparison.compare_start,
                    "compare_end": sp.query_spec.comparison.compare_end,
                }
                if sp.query_spec and sp.query_spec.comparison and sp.query_spec.comparison.enabled else {}
            ),
            "unresolved": sp.unresolved_parts,
        })
        notify("step_done", step.to_dict())

        # coverage_score 过低 → 直接降级 LangChain
        if sp.coverage_score < THRESHOLD_HYBRID:
            if self._semantic_fallback_enabled:
                logger.warning(
                    "[SemanticPipeline] coverage=%.2f < %.2f，触发 LangChain 降级，metrics=%s dimensions=%s unresolved=%s",
                    sp.coverage_score,
                    THRESHOLD_HYBRID,
                    sp.metrics,
                    sp.dimensions,
                    sp.unresolved_parts,
                )
                return self._lc_fallback(
                    question,
                    trace,
                    notify,
                    intent,
                    sp,
                    fallback_reason=f"low_coverage:{sp.coverage_score:.2f}",
                )
            logger.warning(
                "[SemanticPipeline] coverage=%.2f < %.2f，但已关闭 LangChain 降级；继续执行 Semantic 路径。metrics=%s dimensions=%s unresolved=%s",
                sp.coverage_score,
                THRESHOLD_HYBRID,
                sp.metrics,
                sp.dimensions,
                sp.unresolved_parts,
            )

        # ── Step 3: 查询计划 ───────────────────────────────────────────────
        notify("step_start", {"name": "query_plan", "label": "③ 查询规划"})
        step = trace.begin_step("query_plan")
        qp: QueryPlan = self._query_plan.run(sp)
        step.finish(outputs={
            "task_count": len(qp.tasks),
            "execution_mode": qp.execution_mode,
            "tasks": [{"id": t.task_id, "type": t.task_type, "metrics": t.metrics}
                      for t in qp.tasks],
        })
        notify("step_done", step.to_dict())

        # ── Step 4: SQL 生成（逐 task 执行）──────────────────────────────
        sqls: List[str] = []
        last_guard: dict = {"ok": True, "reason": ""}

        for task in qp.tasks:
            if task.task_type == "attribution":
                # Phase 2 占位：跳过 attribution task
                logger.info(f"[SemanticPipeline] attribution task {task.task_id} 跳过（Phase 2）")
                continue

            label = f"④ SQL 生成（{task.task_id}）"
            notify("step_start", {"name": f"sql_gen_{task.task_id}", "label": label})
            step = trace.begin_step(f"sql_gen_{task.task_id}", {"task_id": task.task_id})

            gen_result = self._sql_gen.run(task, sp)
            sql = gen_result["sql"]
            last_guard = {
                "ok": gen_result["guard_ok"],
                "reason": gen_result["guard_reason"],
            }

            step.finish(
                status="ok" if gen_result["guard_ok"] else "error",
                outputs={
                    "sql_preview": sql[:300],
                    "path": gen_result["path"],
                    "guard_ok": gen_result["guard_ok"],
                    "attempts": gen_result["attempts"],
                },
                error="" if gen_result["guard_ok"] else gen_result["guard_reason"],
            )
            notify("step_done", step.to_dict())

            sqls.append(sql)

        final_sql = sqls[0] if sqls else ""
        logger.info("[SemanticPipeline] generated_sql:\n%s", final_sql or "-- empty --")

        trace.finish(
            sql=final_sql,
            error="" if last_guard.get("ok") else last_guard.get("reason", ""),
        )
        self._persist_trace(trace)

        result = self._make_result(
            question=question,
            intent=intent,
            semantic_plan=sp,
            query_plan=qp,
            sql=final_sql,
            path=gen_result.get("path", "semantic") if sqls else "semantic",
            guard=last_guard,
            error="" if last_guard.get("ok") else last_guard.get("reason", ""),
            trace=trace,
        )
        notify("final", result)
        return result

    # ─────────────────────────────────────────────────────────────────────────
    # 内部：LangChain 降级
    # ─────────────────────────────────────────────────────────────────────────

    def _lc_fallback(
        self,
        question: str,
        trace: RequestTrace,
        notify: Callable,
        intent: Optional[IntentPlan] = None,
        sp: Optional[SemanticPlan] = None,
        fallback_reason: str = "",
    ) -> Dict:
        """
        降级到 AskLCPipeline，把已有的语义信息注入 prompt 作为额外上下文。
        """
        notify("step_start", {
            "name": "lc_fallback",
            "label": "⑤ LangChain 降级路径",
            "reason": fallback_reason,
        })
        logger.warning(
            "[SemanticPipeline] 进入 LangChain 降级 question=%r intent=%s coverage=%s reason=%s",
            question,
            intent.intent_type if intent else "",
            f"{sp.coverage_score:.2f}" if sp else "n/a",
            fallback_reason or "unknown",
        )

        lc = self._get_lc_pipeline()

        # 把语义计划作为 hint 注入（通过 prompt_version 机制或直接传 question 增强）
        enhanced_question = question
        if sp and (sp.metrics or sp.dimensions):
            hint_parts = []
            if sp.metrics:
                hint_parts.append(f"相关指标：{', '.join(sp.metrics)}")
            if sp.dimensions:
                hint_parts.append(f"分析维度：{', '.join(sp.dimensions)}")
            if sp.unresolved_parts:
                hint_parts.append(f"注意：{', '.join(sp.unresolved_parts)}")
            if intent and intent.time_hint:
                hint_parts.append(f"时间范围：{intent.time_hint}")
            if hint_parts:
                enhanced_question = question + "\n[语义提示] " + "；".join(hint_parts)

        def lc_step_callback(event: str, data: dict):
            notify(event, {**data, "via": "lc_fallback"})

        lc_result = lc.run_with_trace(
            enhanced_question,
            step_callback=lc_step_callback,
        )

        # 合并 trace
        lc_result["path"] = "lc_fallback"
        lc_result["semantic_coverage"] = sp.coverage_score if sp else 0.0
        lc_result["semantic_fallback_reason"] = fallback_reason
        notify("step_done", {
            "name": "lc_fallback",
            "status": "ok",
            "outputs": {"reason": fallback_reason},
        })
        return lc_result

    # ─────────────────────────────────────────────────────────────────────────
    # 内部：结果构建 & 持久化
    # ─────────────────────────────────────────────────────────────────────────

    def _make_result(
        self,
        question: str,
        intent: IntentPlan,
        sql: str,
        path: str,
        guard: dict,
        error: str,
        trace: RequestTrace,
        semantic_plan: Optional[SemanticPlan] = None,
        query_plan: Optional[QueryPlan] = None,
    ) -> Dict:
        return {
            "question": question,
            "normalized_query": intent.normalized_query,
            "intent": intent.intent_type,
            "business_domain": intent.business_domain,
            "sql": sql,
            "path": path,
            "coverage_score": semantic_plan.coverage_score if semantic_plan else 0.0,
            "metrics": semantic_plan.metrics if semantic_plan else [],
            "dimensions": semantic_plan.dimensions if semantic_plan else [],
            "guard": guard,
            "error": error,
            "trace": trace.to_dict(),
        }

    def _persist_trace(self, trace: RequestTrace) -> None:
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
                    json.dumps(
                        [s.to_dict() for s in trace.steps], ensure_ascii=False
                    ),
                ),
            )
        except Exception as exc:
            logger.warning(f"[SemanticPipeline] trace 持久化失败: {exc}")
