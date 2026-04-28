from __future__ import annotations

import concurrent.futures
import json
import logging
import re
import time as _time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from ..agents.intent_agent import IntentUnderstandingAgent
from ..core.security import assert_readonly_sql, assert_safe_user_request
from ..cube.models import CubeBundle
from ..cube.service import CubeFilter, CubeQuery, CubeService
from ..doris_client import DorisClient
from ..qwen_client import QwenClient
from ..semantic.semantic_sql_rag import SemanticSQLRAGStore
from ..semantic.time_compiler import TimeScopeCompiler
from ..skills.base import SkillContext
from ..tracer import RequestTrace, tracer

logger = logging.getLogger(__name__)


@dataclass
class CubeParsePlan:
    measures: List[str] = field(default_factory=list)
    dimensions: List[str] = field(default_factory=list)
    filters: List[CubeFilter] = field(default_factory=list)
    segments: List[str] = field(default_factory=list)
    derived_metrics: List[Dict[str, Any]] = field(default_factory=list)
    order: List[Dict[str, str]] = field(default_factory=list)
    limit: Optional[int] = 20
    analysis_type: str = "aggregate"
    unresolved: List[str] = field(default_factory=list)
    comparison: Dict[str, Any] = field(default_factory=dict)
    time_scope: Dict[str, Any] = field(default_factory=dict)
    window_topn: Dict[str, Any] = field(default_factory=dict)
    member_compare: Dict[str, Any] = field(default_factory=dict)
    merged_measure_aliases: Dict[str, List[str]] = field(default_factory=dict)


class CubePipeline:
    def __init__(self, config: dict, cube_service: CubeService) -> None:
        self._config = config
        self._cube = cube_service
        self._llm = QwenClient(
            api_key=config["qwen_api_key"],
            model=config.get("model", "qwen-plus"),
            embedding_model=config.get("embedding_model", "text-embedding-v3"),
        )
        self._intent = IntentUnderstandingAgent(self._llm, None)
        self._time = TimeScopeCompiler()
        self._sql_rag_enabled = bool(config.get("semantic_sql_rag_enabled", False))
        self._sem: Optional[DorisClient] = None
        self._trace_db: Optional[DorisClient] = None
        self._semantic_sql_rag_store: Optional[SemanticSQLRAGStore] = None

    def _get_semantic_sql_rag_store(self) -> SemanticSQLRAGStore:
        if self._semantic_sql_rag_store is None:
            conn = dict(
                host=self._config["host"],
                port=self._config["port"],
                user=self._config["user"],
                password=self._config.get("password", ""),
            )
            self._sem = DorisClient(**conn, database="semantic_store")
            self._semantic_sql_rag_store = SemanticSQLRAGStore(
                self._sem,
                self._sem,
                self._llm,
                db_name=self._config.get("database", "retail_dw"),
            )
        return self._semantic_sql_rag_store

    def _get_trace_db(self) -> DorisClient:
        if self._trace_db is None:
            self._trace_db = DorisClient(
                host=self._config["host"],
                port=self._config["port"],
                user=self._config["user"],
                password=self._config.get("password", ""),
                database="vanna_store",
            )
        return self._trace_db

    def run(self, question: str, step_callback=None) -> Dict[str, Any]:
        def _emit(event_type: str, data: dict):
            if step_callback:
                try:
                    step_callback(event_type, data)
                except Exception:
                    pass

        trace = tracer.start(question)
        trace.model_used = self._config.get("model", "qwen-plus")
        _emit("start", {"trace_id": trace.trace_id, "question": question})
        try:
            assert_safe_user_request(question)
            if self._is_attribution_question(question):
                trace.finish(sql="", error="")
                result = {
                    "question": question,
                    "normalized_query": question,
                    "intent": "attribution_analysis",
                    "business_domain": "root_cause_analysis",
                    "sql": "",
                    "path": "smart_rca_redirect",
                    "route": "/rca/smart/stream",
                    "coverage_score": 1.0,
                    "metrics": [],
                    "dimensions": [],
                    "cube_query": {
                        "reason": "归因/异动/流失风险类问题路由到智能归因链路",
                        "stream_endpoint": "/rca/smart/stream",
                    },
                    "guard": {"ok": True, "reason": ""},
                    "error": "",
                    "trace": trace.to_dict(),
                }
                _emit("final", result)
                return result

            # ── Step 1: Cube 模型检查（快，纯内存 / DB 版本比对）──────────────────
            _emit("step_start", {"name": "cube_model_check", "label": "① Cube 模型检查"})
            t0 = _time.time()
            model_step = trace.begin_step("cube_model_check", {})
            self._cube.ensure_models()
            bundle = self._cube.get_bundle()
            model_status = self._cube.get_model_status()
            measure_names   = {m.measure_name for m in bundle.measures  if m.visible}
            dimension_names = {d.dimension_name for d in bundle.dimensions if d.visible}
            segment_names   = {s.segment_name for s in bundle.segments   if s.visible}
            model_outputs = {
                "model_version":  model_status.get("active_version", 0),
                "model_checksum": (model_status.get("active_checksum") or "")[:12],
                "measures":       len(measure_names),
                "dimensions":     len(dimension_names),
                "segments":       len(segment_names),
                "joins":          len(bundle.joins),
            }
            model_step.finish(outputs=model_outputs)
            _emit("step_done", {
                "name": "cube_model_check",
                "status": "ok",
                "duration_ms": round((_time.time() - t0) * 1000, 1),
                "outputs": model_outputs,
            })

            # ── Steps 2 + 3: 意图理解 & SQL RAG 并行 ──────────────────────────
            # 同时 step_start，让 UI 显示两张卡片都在转圈
            _emit("step_start", {"name": "intent",           "label": "② 意图理解"})
            _emit("step_start", {"name": "semantic_sql_rag", "label": "③ SQL RAG 召回"})

            intent_step = trace.begin_step("intent", {"question": question})
            rag_step    = trace.begin_step("semantic_sql_rag", {}) if self._sql_rag_enabled else None

            def _run_intent():
                return self._intent.run(question)

            def _run_rag():
                if not self._sql_rag_enabled:
                    return []
                return self._get_semantic_sql_rag_store().search(
                    question,
                    top_k=min(self._config.get("n_results", 5), 5),
                )

            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                intent_future = pool.submit(_run_intent)
                rag_future    = pool.submit(_run_rag)
                intent        = intent_future.result()
                rag_examples  = rag_future.result()

            intent_outputs = {
                "intent_type":      intent.intent_type,
                "business_domain":  intent.business_domain,
                "complexity":       intent.complexity,
                "time_hint":        intent.time_hint,
                "normalized_query": intent.normalized_query or "",
            }
            intent_step.finish(outputs=intent_outputs)
            _emit("step_done", {
                "name": "intent", "status": "ok",
                "duration_ms": round(intent_step.duration_ms, 1),
                "outputs": intent_outputs,
            })

            if self._sql_rag_enabled:
                rag_outputs = {
                    "enabled":       True,
                    "count":         len(rag_examples),
                    "top_questions": [item.get("raw_question", "")   for item in rag_examples[:3]],
                    "top_sqls":      [item.get("sql_text", "")[:200] for item in rag_examples[:3]],
                    "top_scores":    [round(float(item.get("similarity", 0) or 0), 4) for item in rag_examples[:3]],
                    "top_distances":  [round(float(item.get("dist", 0) or 0), 4) for item in rag_examples[:3]],
                    "quality_scores": [round(float(item.get("quality_score", 0) or 0), 4) for item in rag_examples[:3]],
                }
                rag_step.finish(outputs=rag_outputs)
                _emit("step_done", {
                    "name": "semantic_sql_rag", "status": "ok",
                    "duration_ms": round(rag_step.duration_ms, 1),
                    "outputs": rag_outputs,
                })
            else:
                _emit("step_done", {
                    "name": "semantic_sql_rag", "status": "cached",
                    "note": "DISABLED", "duration_ms": 0,
                    "outputs": {"enabled": False, "count": 0},
                })

            # ── Step 4: Prompt 构建（纯字符串拼接，<10ms）────────────────────────
            _emit("step_start", {"name": "cube_prompt", "label": "④ Prompt 构建"})
            t_prompt = _time.time()
            rule_plan = self._try_rule_plan(
                question,
                intent.normalized_query or question,
                intent.time_hint,
                bundle,
                measure_names,
                dimension_names,
                segment_names,
            )
            prompt = self._build_parse_prompt(
                question,
                intent.normalized_query or question,
                intent.time_hint,
                bundle,
                rag_examples,
            )
            prompt_ms = round((_time.time() - t_prompt) * 1000, 1)
            prompt_outputs = {
                "prompt_chars":     len(prompt),
                "measures_in_prompt":   len(measure_names),
                "dimensions_in_prompt": len(dimension_names),
                "rag_examples":     len(rag_examples),
                "time_hint":        intent.time_hint or "—",
                "rule_plan_hit":    bool(rule_plan),
            }
            _emit("step_done", {
                "name": "cube_prompt", "status": "ok",
                "duration_ms": prompt_ms,
                "outputs": prompt_outputs,
            })

            # ── Step 5: LLM 语义解析（主要耗时点：~1-4s）─────────────────────────
            _emit("step_start", {"name": "cube_llm_parse", "label": "⑤ LLM 语义解析"})
            llm_step = trace.begin_step("cube_llm_parse", {})
            if rule_plan:
                data = {
                    "measures": rule_plan.measures,
                    "dimensions": rule_plan.dimensions,
                    "filters": [
                        {"member": f.member, "operator": f.operator, "values": f.values}
                        for f in rule_plan.filters
                    ],
                    "segments": rule_plan.segments,
                    "order": rule_plan.order,
                    "limit": rule_plan.limit,
                    "unresolved": rule_plan.unresolved,
                }
                llm_outputs = {
                    "skipped": True,
                    "reason": "rule_plan_hit",
                    "raw_measures": data["measures"],
                    "raw_dimensions": data["dimensions"],
                    "raw_segments": data["segments"],
                    "raw_filters": data["filters"],
                    "raw_limit": data["limit"],
                }
                llm_step.finish(status="cached", outputs=llm_outputs)
                _emit("step_done", {
                    "name": "cube_llm_parse", "status": "cached",
                    "note": "RULE",
                    "duration_ms": round(llm_step.duration_ms, 1),
                    "outputs": llm_outputs,
                })
            else:
                try:
                    raw = self._llm.generate(prompt, temperature=0.0).strip()
                    raw_clean = re.sub(r"^```(?:json)?\s*", "", raw)
                    raw_clean = re.sub(r"\s*```$", "", raw_clean).strip()
                    data = json.loads(raw_clean)
                    llm_outputs = {
                        "response_chars": len(raw),
                        "raw_measures":   data.get("measures", []),
                        "raw_dimensions": data.get("dimensions", []),
                        "raw_segments":   data.get("segments", []),
                        "raw_filters":    data.get("filters", []),
                        "raw_limit":      data.get("limit"),
                    }
                    llm_step.finish(outputs=llm_outputs)
                    _emit("step_done", {
                        "name": "cube_llm_parse", "status": "ok",
                        "duration_ms": round(llm_step.duration_ms, 1),
                        "outputs": llm_outputs,
                    })
                except Exception as exc:
                    llm_step.finish(status="error", error=str(exc))
                    _emit("step_done", {
                        "name": "cube_llm_parse", "status": "error",
                        "duration_ms": round(llm_step.duration_ms, 1),
                        "outputs": {}, "error": str(exc),
                    })
                    raise

            # ── Step 6: 规则修正 & 时间范围解析（<50ms）──────────────────────────
            _emit("step_start", {"name": "cube_heuristics", "label": "⑥ 规则修正"})
            t_heur = _time.time()
            plan = self._build_plan_from_data(
                data, question,
                intent.normalized_query or question,
                intent.time_hint,
                bundle, rag_examples,
                measure_names, dimension_names, segment_names,
            )
            heur_ms = round((_time.time() - t_heur) * 1000, 1)
            heur_outputs = {
                "measures":      plan.measures,
                "dimensions":    plan.dimensions,
                "segments":      plan.segments,
                "derived_metrics": plan.derived_metrics,
                "analysis_type": plan.analysis_type,
                "filters":       [{"member": f.member, "op": f.operator, "values": f.values} for f in plan.filters],
                "time_scope":    plan.time_scope,
                "comparison":    plan.comparison,
                "window_topn":    plan.window_topn,
                "member_compare": plan.member_compare,
                "merged_measure_aliases": plan.merged_measure_aliases,
                "limit":         plan.limit,
                "unresolved":    plan.unresolved,
            }
            _emit("step_done", {
                "name": "cube_heuristics", "status": "ok",
                "duration_ms": heur_ms,
                "outputs": heur_outputs,
            })

            # ── Step 7: SQL 编译（纯 Python 字符串拼接，<5ms）────────────────────
            _emit("step_start", {"name": "cube_compile", "label": "⑦ SQL 编译"})
            cube_step = trace.begin_step("cube_compile", {})

            if not plan.measures:
                raise ValueError(
                    f"无法识别指标，可用: {sorted(measure_names)}"
                )

            if plan.member_compare.get("enabled"):
                final_sql = self._build_member_compare_sql(plan)
                cube_query_payload = {
                    "mode": "member_binary_compare",
                    "base_measures": plan.measures,
                    "filters": [
                        {"member": f.member, "operator": f.operator, "values": f.values}
                        for f in plan.filters
                    ],
                    "member_compare": plan.member_compare,
                }
            elif plan.comparison.get("enabled"):
                final_sql = self._build_comparison_sql(plan)
                cube_query_payload = {
                    "mode": "comparison",
                    "base_measures": plan.measures,
                    "dimensions":    plan.dimensions,
                    "comparison":    plan.comparison,
                    "time_scope":    plan.time_scope,
                }
            else:
                cube_query = CubeQuery(
                    metrics=plan.measures,
                    dimensions=plan.dimensions,
                    filters=plan.filters,
                    segments=plan.segments,
                    derived_metrics=plan.derived_metrics,
                    order=[] if plan.window_topn else plan.order,
                    limit=None if plan.window_topn else plan.limit,
                    rag_hints=[
                        {"question": item.get("raw_question", ""),
                         "sql":      item.get("sql_text", "")}
                        for item in rag_examples[:5]
                    ],
                )
                cube_result       = self._cube.generate_sql(cube_query)
                final_sql         = cube_result["sql"]
                cube_query_payload = cube_result.get("cube_query", {})
                if plan.window_topn:
                    final_sql = self._wrap_window_topn(final_sql, plan)
                    cube_query_payload = self._with_window_topn_payload(
                        cube_query_payload,
                        plan.window_topn,
                    )

            compile_outputs = {
                "path":           "cube_member_compare" if plan.member_compare.get("enabled") else ("cube_compare" if plan.comparison.get("enabled") else "cube_poc"),
                "sql_full":       final_sql,
                "sql_lines":      final_sql.count("\n") + 1,
                "model_version":  model_status.get("active_version", 0),
                "cube_query":     cube_query_payload,
            }
            cube_step.finish(outputs=compile_outputs)
            _emit("step_done", {
                "name": "cube_compile", "status": "ok",
                "duration_ms": round(cube_step.duration_ms, 1),
                "outputs": compile_outputs,
            })

            assert_readonly_sql(final_sql)
            trace.finish(sql=final_sql, error="")
            self._persist_trace(trace)
            result = {
                "question":        question,
                "normalized_query": intent.normalized_query,
                "intent":          intent.intent_type,
                "business_domain": intent.business_domain,
                "sql":             final_sql,
                "path":            "cube",
                "coverage_score":  1.0 if plan.measures else 0.0,
                "metrics":         plan.measures,
                "dimensions":      plan.dimensions,
                "cube_query":      cube_query_payload,
                "guard":           {"ok": True, "reason": ""},
                "error":           "",
                "trace":           trace.to_dict(),
            }
            _emit("final", result)
            return result

        except Exception as exc:
            logger.exception("[CubePipeline] 失败: %s", exc)
            trace.finish(sql="", error=str(exc))
            self._persist_trace(trace)
            result = {
                "question": question, "sql": "", "path": "cube",
                "guard": {"ok": False, "reason": str(exc)},
                "error": str(exc), "trace": trace.to_dict(),
            }
            _emit("final", result)
            return result


    def _build_plan_from_data(
        self,
        data: Dict[str, Any],
        question: str,
        normalized_query: str,
        time_hint: str,
        bundle: CubeBundle,
        rag_examples: List[Dict[str, Any]],
        measure_names: set,
        dimension_names: set,
        segment_names: set,
    ) -> CubeParsePlan:
        """LLM 原始 JSON → CubeParsePlan（规则修正 + 时间解析）。"""
        raw_measures = self._normalize_name_list(data.get("measures", []))
        raw_dimensions = self._normalize_name_list(data.get("dimensions", []))
        raw_segments = self._normalize_name_list(data.get("segments", []))
        plan = CubeParsePlan(
            measures=[
                name for name in raw_measures
                if name in measure_names
            ] or self._fuzzy_resolve_measures(raw_measures, measure_names),
            dimensions=[
                name for name in raw_dimensions
                if name in dimension_names
            ],
            segments=[
                name for name in raw_segments
                if name in segment_names
            ],
            order=[
                {
                    "member":    str(item.get("member", "")),
                    "direction": (item.get("direction") or "DESC").upper(),
                }
                for item in data.get("order", [])
                if isinstance(item, dict)
                and isinstance(item.get("member"), str)
                and item.get("member") in (measure_names | dimension_names)
            ],
            limit=self._safe_int(data.get("limit", 20), 20),
            unresolved=[str(item) for item in data.get("unresolved", [])],
        )
        for item in data.get("filters", []):
            if not isinstance(item, dict):
                continue
            member = item.get("member", "")
            if not isinstance(member, str):
                logger.warning("[CubePipeline] filter.member 非字符串，跳过: %r", member)
                continue
            member = member.strip()
            if member not in dimension_names:
                continue
            values = item.get("values", [])
            if isinstance(values, dict):
                values = list(values.values())
            elif not isinstance(values, list):
                values = [values]
            plan.filters.append(CubeFilter(
                member=member,
                operator=str(item.get("operator", "equals")),
                values=values,
            ))
        self._apply_heuristics(question, bundle, plan)
        parsed_time = self._time.build(question, self._intent._fallback_plan(question, time_hint))
        if parsed_time.time_scope:
            plan.time_scope = {
                "start": parsed_time.time_scope.start,
                "end":   parsed_time.time_scope.end,
                "label": parsed_time.time_scope.label,
            }
            if "dt" in dimension_names:
                plan.filters = [f for f in plan.filters if f.member != "dt"]
                plan.filters.append(CubeFilter(
                    member="dt", operator="between",
                    values=[parsed_time.time_scope.start, parsed_time.time_scope.end],
                ))
        else:
            self._apply_default_time_scope(plan, dimension_names)
        if parsed_time.analysis_type:
            plan.analysis_type = parsed_time.analysis_type
        if parsed_time.comparison and parsed_time.comparison.enabled:
            plan.comparison = {
                "enabled":       True,
                "mode":          parsed_time.comparison.mode,
                "compare_start": parsed_time.comparison.compare_start,
                "compare_end":   parsed_time.comparison.compare_end,
                "label":         parsed_time.comparison.label,
            }
        if plan.analysis_type == "trend" and "time_month" in dimension_names and "time_month" not in plan.dimensions:
            plan.dimensions.insert(0, "time_month")
        self._apply_trend_order(plan)
        if plan.analysis_type == "ranking" and not plan.order and plan.measures:
            plan.order = [{"member": plan.measures[0], "direction": "DESC"}]
        self._apply_global_topn(question, plan)
        self._normalize_display_dimensions(question, plan, dimension_names)
        self._apply_window_topn(question, plan, dimension_names)
        return plan

    def _apply_heuristics(
        self,
        question: str,
        bundle: CubeBundle,
        plan: CubeParsePlan,
        *,
        allow_measure_fallback: bool = True,
    ) -> None:
        q = question or ""
        measure_names = {m.measure_name for m in bundle.measures if m.visible}
        dimension_names = {d.dimension_name for d in bundle.dimensions if d.visible}
        segment_names = {s.segment_name for s in bundle.segments if s.visible}

        self._apply_semantic_aliases(
            q,
            bundle,
            plan,
            measure_names,
            dimension_names,
            segment_names,
        )
        self._normalize_mutually_exclusive_segments(plan, dimension_names)

        # last resort: use first visible measure so we don't crash
        if not plan.measures and allow_measure_fallback and measure_names:
            fallback = sorted(measure_names)[0]
            logger.warning("[CubePipeline] measures 为空，兜底使用 %s", fallback)
            plan.measures.append(fallback)

        self._apply_alias_value_filters(q, bundle, plan)
        self._apply_enum_filters(q, bundle, plan)
        self._normalize_compare_dimensions(q, plan, dimension_names)
        self._normalize_member_exact_filter(q, plan)
        self._apply_subset_derived_metrics(q, bundle, plan, measure_names)
        self._normalize_subset_metrics(q, plan, measure_names)
        self._normalize_exact_filter_dimensions(q, plan)
        self._apply_member_binary_compare(q, plan)
        self._record_merged_measure_aliases(q, bundle, plan)

    def _normalize_mutually_exclusive_segments(
        self,
        plan: CubeParsePlan,
        dimension_names: set,
    ) -> None:
        if "member_type" not in dimension_names:
            return
        if {"plus_members", "normal_members"}.issubset(set(plan.segments)):
            plan.segments = [
                item for item in plan.segments
                if item not in {"plus_members", "normal_members"}
            ]
            if "member_type" not in plan.dimensions:
                plan.dimensions.append("member_type")

    def _normalize_compare_dimensions(
        self,
        question: str,
        plan: CubeParsePlan,
        dimension_names: set,
    ) -> None:
        q = question or ""
        if "member_type" not in dimension_names:
            return
        if not any(k in q for k in ["对比", "比较", "分别", "各"]):
            return
        if not any(k in q for k in ["会员", "PLUS", "plus", "普通会员", "非会员"]):
            return
        plan.segments = [
            item for item in plan.segments
            if item not in {"plus_members", "normal_members"}
        ]
        plan.filters = [
            item for item in plan.filters
            if item.member not in {"member_type", "member_type_code", "is_plus_vip"}
        ]
        if "member_type" not in plan.dimensions:
            plan.dimensions.append("member_type")

    def _apply_semantic_aliases(
        self,
        question: str,
        bundle: CubeBundle,
        plan: CubeParsePlan,
        measure_names: set,
        dimension_names: set,
        segment_names: set,
    ) -> None:
        for alias in bundle.aliases:
            if not alias.visible or not self._alias_matches(question, alias.alias_text, alias.match_type):
                continue
            if alias.entity_type == "measure" and alias.entity_name in measure_names:
                if alias.entity_name not in plan.measures:
                    plan.measures.append(alias.entity_name)
            elif alias.entity_type == "dimension" and alias.entity_name in dimension_names:
                if alias.entity_name not in plan.dimensions:
                    plan.dimensions.append(alias.entity_name)
            elif alias.entity_type == "segment" and alias.entity_name in segment_names:
                if alias.entity_name not in plan.segments:
                    plan.segments.append(alias.entity_name)

    def _alias_matches(self, question: str, alias_text: str, match_type: str = "contains") -> bool:
        text = str(alias_text or "").strip()
        if not text:
            return False
        mode = (match_type or "contains").lower()
        if mode == "exact":
            return question.strip() == text
        if mode == "regex":
            try:
                return re.search(text, question) is not None
            except re.error:
                return False
        return text in question

    def _dimension_aliases(self, bundle: CubeBundle, dimension_name: str) -> List[str]:
        values: List[str] = []
        for dim in bundle.dimensions:
            if dim.dimension_name == dimension_name:
                values.extend([dim.dimension_name, dim.title])
        values.extend(
            alias.alias_text
            for alias in bundle.aliases
            if alias.visible and alias.entity_type == "dimension" and alias.entity_name == dimension_name
        )
        result: List[str] = []
        seen: set[str] = set()
        for item in values:
            text = str(item or "").strip()
            if text and text not in seen:
                seen.add(text)
                result.append(text)
        return sorted(result, key=len, reverse=True)

    def _apply_alias_value_filters(
        self,
        question: str,
        bundle: CubeBundle,
        plan: CubeParsePlan,
    ) -> None:
        existing = {f.member for f in plan.filters}
        dimensions = {d.dimension_name: d for d in bundle.dimensions if d.visible}
        for dimension_name in list(plan.dimensions):
            if dimension_name in existing:
                continue
            dim = dimensions.get(dimension_name)
            if dim is None:
                continue
            for alias_text in self._dimension_aliases(bundle, dimension_name):
                m = re.search(
                    rf"{re.escape(alias_text)}\s*(?:为|是|=|:|：)\s*([A-Za-z0-9_\-]+|[\u4e00-\u9fa5]{{1,20}})",
                    question,
                )
                if not m:
                    continue
                raw_value = re.split(r"[，,。；;、\s]|的", m.group(1).strip(), maxsplit=1)[0]
                if self._is_question_value(raw_value):
                    continue
                if raw_value and raw_value != alias_text:
                    plan.filters.append(
                        CubeFilter(member=dimension_name, operator="equals", values=[raw_value])
                    )
                    existing.add(dimension_name)
                    break

    def _apply_enum_filters(
        self,
        question: str,
        bundle: CubeBundle,
        plan: CubeParsePlan,
    ) -> None:
        existing = {f.member for f in plan.filters}
        primary_by_cube = {
            dim.cube_name: dim.dimension_name
            for dim in bundle.dimensions
            if dim.visible and dim.primary_key_flag
        }
        for dim in bundle.dimensions:
            filter_member = self._enum_filter_member(dim, primary_by_cube)
            if not dim.visible or not dim.enum_mapping or filter_member in existing:
                continue
            dimension_cued = any(
                alias in question
                for alias in self._dimension_aliases(bundle, dim.dimension_name)
            )
            filter_values: List[str] = []
            seen_codes: set[str] = set()
            for label, code in dim.enum_mapping.items():
                label_text = str(label or "").strip()
                code_text = str(code or "").strip()
                if not label_text or label_text not in question or not code_text:
                    continue
                if _looks_like_code_value(label_text) and not dimension_cued:
                    continue
                if code_text in seen_codes:
                    continue
                seen_codes.add(code_text)
                filter_values.append(code_text)
            if not filter_values:
                continue
            plan.filters.append(
                CubeFilter(
                    member=filter_member,
                    operator="in" if len(filter_values) > 1 else "equals",
                    values=filter_values,
                )
            )
            if len(filter_values) > 1 and dim.dimension_name not in plan.dimensions:
                plan.dimensions.append(dim.dimension_name)
            if len(filter_values) == 1 and not self._asks_group_by_dimension(question):
                plan.dimensions = [name for name in plan.dimensions if name != dim.dimension_name]
            existing.add(filter_member)

    def _enum_filter_member(self, dim: Any, primary_by_cube: Dict[str, str]) -> str:
        primary = primary_by_cube.get(dim.cube_name)
        if primary and dim.dimension_name in {"city_name", "member_type", "store_name"}:
            return primary
        return dim.dimension_name

    def _asks_group_by_dimension(self, question: str) -> bool:
        return any(k in (question or "") for k in ["各", "每个", "分别", "分布", "对比", "按"])

    def _normalize_exact_filter_dimensions(self, question: str, plan: CubeParsePlan) -> None:
        if self._asks_group_by_dimension(question):
            return
        exact_members = {
            flt.member
            for flt in plan.filters
            if (flt.operator or "equals").lower() in {"equals", "in"} and len(flt.values or []) == 1
        }
        if not exact_members:
            return
        equivalent_dims = set(exact_members)
        if "city_code" in exact_members:
            equivalent_dims.add("city_name")
        if "member_type_code" in exact_members:
            equivalent_dims.add("member_type")
        if "store_id" in exact_members:
            equivalent_dims.add("store_name")
        plan.dimensions = [name for name in plan.dimensions if name not in equivalent_dims]

    def _apply_member_binary_compare(self, question: str, plan: CubeParsePlan) -> None:
        q = question or ""
        if not any(k in q for k in ["对比值", "差值", "相差", "比例"]):
            return
        if not any(k in q for k in ["会员", "非会员", "普通会员", "PLUS", "plus"]):
            return
        if "gmv" not in plan.measures:
            return
        plan.member_compare = {
            "enabled": True,
            "base_measure": "gmv",
            "positive": {"label": "会员", "member": "member_type_code", "value": "1"},
            "negative": {"label": "非会员", "member": "member_type_code", "value": "0"},
            "outputs": ["positive", "negative", "delta", "ratio"],
        }
        plan.dimensions = [name for name in plan.dimensions if name != "member_type"]
        plan.filters = [
            flt for flt in plan.filters
            if flt.member not in {"member_type", "member_type_code", "is_plus_vip"}
        ]
        plan.segments = [
            item for item in plan.segments
            if item not in {"plus_members", "normal_members"}
        ]

    def _record_merged_measure_aliases(
        self,
        question: str,
        bundle: CubeBundle,
        plan: CubeParsePlan,
    ) -> None:
        merged: Dict[str, List[str]] = {}
        for alias in bundle.aliases:
            if alias.entity_type != "measure" or alias.entity_name not in plan.measures or not alias.visible:
                continue
            if self._alias_matches(question, alias.alias_text, alias.match_type):
                merged.setdefault(alias.entity_name, []).append(alias.alias_text)
        for name in plan.measures:
            values = sorted(set(merged.get(name, [])))
            if len(values) > 1:
                plan.merged_measure_aliases[name] = values

    def _is_question_value(self, value: str) -> bool:
        text = str(value or "").strip()
        if not text:
            return True
        interrogatives = {
            "哪些", "哪个", "哪家", "哪几", "多少", "几", "几家", "几个",
            "什么", "怎样", "如何", "怎么", "排名", "排行",
        }
        return text in interrogatives or any(text.startswith(prefix) for prefix in ["哪些", "哪个", "多少"])

    def _is_member_compare_question(self, question: str) -> bool:
        q = question or ""
        return any(k in q for k in ["对比", "比较", "分别", "各"]) and any(
            k in q for k in ["会员", "PLUS", "plus", "普通会员", "非会员"]
        )

    def _normalize_member_exact_filter(self, question: str, plan: CubeParsePlan) -> None:
        if self._is_member_compare_question(question):
            return
        q = question or ""
        target: Optional[str] = None
        if re.search(r"PLUS\s*会员|plus\s*会员", q, flags=re.IGNORECASE) or "高价值会员" in q:
            target = "1"
        elif "普通会员" in q or "非会员" in q:
            target = "0"
        if target is None:
            has_member_key_filter = any(f.member in {"member_type_code", "is_plus_vip"} for f in plan.filters)
            if has_member_key_filter:
                plan.segments = [
                    item for item in plan.segments
                    if item not in {"plus_members", "normal_members"}
                ]
            return

        plan.segments = [
            item for item in plan.segments
            if item not in {"plus_members", "normal_members"}
        ]
        plan.filters = [
            item for item in plan.filters
            if item.member not in {"member_type", "member_type_code", "is_plus_vip"}
        ]
        plan.filters.append(CubeFilter(member="member_type_code", operator="equals", values=[target]))
        if not self._asks_group_by_dimension(q):
            plan.dimensions = [name for name in plan.dimensions if name != "member_type"]

    def _normalize_subset_metrics(
        self,
        question: str,
        plan: CubeParsePlan,
        measure_names: set,
    ) -> None:
        q = question or ""
        if "其中" not in q:
            return
        if plan.derived_metrics:
            return
        if "plus_consume_amt" not in measure_names:
            return
        if not (re.search(r"PLUS\s*会员|plus\s*会员", q, flags=re.IGNORECASE) or "高价值会员" in q):
            return
        if "gmv" in measure_names and "gmv" not in plan.measures:
            plan.measures.insert(0, "gmv")
        if "plus_consume_amt" not in plan.measures:
            plan.measures.append("plus_consume_amt")
        plan.segments = [
            item for item in plan.segments
            if item not in {"plus_members", "normal_members"}
        ]
        plan.filters = [
            item for item in plan.filters
            if item.member not in {"member_type", "member_type_code", "is_plus_vip"}
        ]
        plan.dimensions = [name for name in plan.dimensions if name != "member_type"]

    def _apply_subset_derived_metrics(
        self,
        question: str,
        bundle: CubeBundle,
        plan: CubeParsePlan,
        measure_names: set,
    ) -> None:
        q = question or ""
        subset_text = self._subset_text_for_derived_metric(q)
        if not subset_text:
            return
        base_measure = "gmv" if "gmv" in measure_names else (plan.measures[0] if plan.measures else "")
        if not base_measure:
            return
        conditions, alias_prefix = self._extract_subset_conditions(subset_text, bundle)
        if not conditions:
            return

        if base_measure not in plan.measures:
            plan.measures.insert(0, base_measure)
        legacy_conditional_measures = {"plus_consume_amt"}
        plan.measures = [name for name in plan.measures if name not in legacy_conditional_measures]
        if base_measure not in plan.measures:
            plan.measures.insert(0, base_measure)
        plan.filters = [
            flt for flt in plan.filters
            if not self._same_filter_any(flt, conditions)
        ]
        condition_members = {item["member"] for item in conditions}
        plan.dimensions = [
            name for name in plan.dimensions
            if name not in condition_members and not (
                name == "member_type" and "member_type_code" in condition_members
            )
        ]
        plan.segments = [
            item for item in plan.segments
            if item not in {"plus_members", "normal_members"}
        ]

        subset_alias = f"{alias_prefix}_{base_measure}" if alias_prefix else f"subset_{base_measure}"
        derived = {
            "type": "subset",
            "alias": self._safe_alias(subset_alias),
            "base_measure": base_measure,
            "conditions": conditions,
        }
        plan.derived_metrics = [
            item for item in plan.derived_metrics
            if item.get("alias") != derived["alias"]
        ]
        plan.derived_metrics.append(derived)
        if any(k in q for k in ["占比", "比例", "占全国", "占整体", "占总"]):
            plan.derived_metrics.append({
                "type": "ratio",
                "alias": self._safe_alias(f"{subset_alias}_ratio"),
                "base_measure": base_measure,
                "conditions": conditions,
            })

    def _subset_text_for_derived_metric(self, question: str) -> str:
        q = question or ""
        if "其中" in q:
            return q.split("其中", 1)[1]
        if not any(k in q for k in ["占总", "占总金额", "占比", "比例"]):
            return ""
        parts = re.split(r"[，,。；;？?]", q)
        for idx, part in enumerate(parts):
            if any(k in part for k in ["占总", "占比", "比例"]):
                candidates = []
                if idx > 0:
                    candidates.append(parts[idx - 1])
                candidates.append(part)
                return "，".join(item for item in candidates if item)
        return q

    def _extract_subset_conditions(
        self,
        subset_text: str,
        bundle: CubeBundle,
    ) -> tuple[List[Dict[str, Any]], str]:
        text = subset_text or ""
        primary_by_cube = {
            dim.cube_name: dim.dimension_name
            for dim in bundle.dimensions
            if dim.visible and dim.primary_key_flag
        }
        if (
            "会员" in text
            and not any(k in text for k in ["普通会员", "非会员"])
            and not re.search(r"PLUS\s*会员|plus\s*会员", text, flags=re.IGNORECASE)
        ):
            return ([{"member": "member_type_code", "operator": "equals", "values": ["1"]}], "member")
        if re.search(r"PLUS\s*会员|plus\s*会员", text, flags=re.IGNORECASE) or "高价值会员" in text:
            return ([{"member": "member_type_code", "operator": "equals", "values": ["1"]}], "plus_member")
        if "普通会员" in text or "非会员" in text:
            return ([{"member": "member_type_code", "operator": "equals", "values": ["0"]}], "normal_member")

        matched: Dict[str, Dict[str, Any]] = {}
        alias_prefix = ""
        for dim in bundle.dimensions:
            if not dim.visible or not dim.enum_mapping:
                continue
            filter_member = self._enum_filter_member(dim, primary_by_cube)
            for label, code in dim.enum_mapping.items():
                label_text = str(label or "").strip()
                code_text = str(code or "").strip()
                if not label_text or not code_text or label_text not in text:
                    continue
                entry = matched.setdefault(
                    filter_member,
                    {"member": filter_member, "operator": "in", "values": []},
                )
                if code_text not in entry["values"]:
                    entry["values"].append(code_text)
                if not alias_prefix:
                    alias_prefix = self._alias_prefix_for_subset(dim.dimension_name, code_text, label_text)
        conditions: List[Dict[str, Any]] = []
        for item in matched.values():
            if len(item["values"]) == 1:
                item["operator"] = "equals"
            conditions.append(item)
        return conditions, alias_prefix or "subset"

    def _alias_prefix_for_subset(self, dimension_name: str, code: str, label: str) -> str:
        if dimension_name == "city_name":
            return str(code or "city").lower()
        if dimension_name == "member_type":
            return "member"
        if dimension_name == "store_type":
            return "store_type"
        return re.sub(r"[^A-Za-z0-9_]+", "_", str(code or label or "subset").lower()).strip("_") or "subset"

    def _safe_alias(self, alias: str) -> str:
        value = re.sub(r"[^A-Za-z0-9_]+", "_", str(alias or "")).strip("_").lower()
        if not value:
            value = "derived_metric"
        if value[0].isdigit():
            value = f"m_{value}"
        return value[:64]

    def _same_filter_any(self, flt: CubeFilter, conditions: List[Dict[str, Any]]) -> bool:
        flt_values = {str(item) for item in (flt.values or [])}
        for cond in conditions:
            if flt.member != cond.get("member"):
                continue
            cond_values = {str(item) for item in (cond.get("values") or [])}
            if flt_values and cond_values and flt_values.issubset(cond_values):
                return True
        return False

    def _parse_top_n(self, question: str) -> Optional[int]:
        q = question or ""
        m = re.search(r"(?:前|top\s*)(\d+)", q, flags=re.IGNORECASE)
        if m:
            return max(1, min(int(m.group(1)), 100))
        zh_map = {
            "一": 1, "二": 2, "两": 2, "三": 3, "四": 4, "五": 5,
            "六": 6, "七": 7, "八": 8, "九": 9, "十": 10,
        }
        m = re.search(r"前([一二两三四五六七八九十])", q)
        if m:
            return zh_map.get(m.group(1), 10)
        if "第一" in q:
            return 1
        return None

    def _apply_global_topn(self, question: str, plan: CubeParsePlan) -> None:
        if not plan.measures:
            return
        top_n = self._parse_top_n(question)
        q = question or ""
        ranking_words = ["排名", "排行", "最高", "最低", "最好", "卖得最好"]
        if top_n is None and not any(word in q for word in ranking_words):
            return
        metric = plan.order[0]["member"] if plan.order else plan.measures[0]
        if metric not in plan.measures:
            metric = plan.measures[0]
        direction = "ASC" if "最低" in q else "DESC"
        plan.order = [{"member": metric, "direction": direction}]
        if top_n is not None:
            plan.limit = top_n

    def _apply_trend_order(self, plan: CubeParsePlan) -> None:
        if plan.analysis_type != "trend" or plan.order:
            return
        for candidate in ("time_month", "dt"):
            if candidate in plan.dimensions:
                plan.order = [{"member": candidate, "direction": "ASC"}]
                return

    def _apply_default_time_scope(self, plan: CubeParsePlan, dimension_names: set) -> None:
        mode = str(self._config.get("cube_default_time_scope", "") or "").strip().lower()
        if not mode or mode in {"none", "off", "disabled"} or "dt" not in dimension_names:
            return
        if any(flt.member == "dt" for flt in plan.filters):
            return
        today = datetime.now().date()
        label = mode
        if mode in {"this_month", "本月"}:
            start = today.replace(day=1)
            end = today
            label = "本月"
        elif mode in {"last_month", "上个月", "上月"}:
            ref = today.replace(day=1) - timedelta(days=1)
            start = ref.replace(day=1)
            end = ref
            label = "上个月"
        else:
            m = re.match(r"^(?:last_|最近|近)?(\d+)_?days?$", mode)
            if not m:
                return
            days = max(1, int(m.group(1)))
            start = today - timedelta(days=days - 1)
            end = today
            label = f"近{days}天"
        plan.time_scope = {"start": start.isoformat(), "end": end.isoformat(), "label": label, "defaulted": True}
        plan.filters.append(CubeFilter(member="dt", operator="between", values=[start.isoformat(), end.isoformat()]))

    def _is_attribution_question(self, question: str) -> bool:
        q = question or ""
        return any(
            word in q
            for word in ["归因", "原因", "为什么", "异常", "异动", "波动", "影响因素", "流失风险", "深度分析"]
        )

    def _apply_window_topn(
        self,
        question: str,
        plan: CubeParsePlan,
        dimension_names: set,
    ) -> None:
        q = question or ""
        if not plan.measures or len(plan.dimensions) < 2:
            return
        if not any(k in q for k in ["排名", "排行", "Top", "top", "最高", "第一", "前三", "前3", "前十", "前10"]):
            return
        partition_dim = ""
        if any(k in q for k in ["各地区", "每个地区", "各城市", "每个城市"]):
            for candidate in ("city_name", "region_name", "city_code"):
                if candidate in plan.dimensions:
                    partition_dim = candidate
                    break
        elif any(k in q for k in ["各门店类型", "每个门店类型"]) and "store_type" in plan.dimensions:
            partition_dim = "store_type"
        if not partition_dim:
            return

        top_n = self._parse_top_n(q) or 1

        metric = plan.order[0]["member"] if plan.order else plan.measures[0]
        if metric not in plan.measures:
            metric = plan.measures[0]
        plan.window_topn = {
            "partition_by": partition_dim,
            "order_by": metric,
            "direction": "DESC",
            "top_n": top_n,
        }
        plan.order = [{"member": metric, "direction": "DESC"}]
        plan.limit = None

    def _try_rule_plan(
        self,
        question: str,
        normalized_query: str,
        time_hint: str,
        bundle: CubeBundle,
        measure_names: set,
        dimension_names: set,
        segment_names: set,
    ) -> Optional[CubeParsePlan]:
        plan = CubeParsePlan()
        self._apply_heuristics(question, bundle, plan, allow_measure_fallback=False)
        if not plan.measures:
            return None
        parsed_time = self._time.build(
            question,
            self._intent._fallback_plan(question, time_hint),
        )
        if parsed_time.time_scope and "dt" in dimension_names:
            plan.time_scope = {
                "start": parsed_time.time_scope.start,
                "end": parsed_time.time_scope.end,
                "label": parsed_time.time_scope.label,
            }
            plan.filters.append(
                CubeFilter(
                    member="dt",
                    operator="between",
                    values=[parsed_time.time_scope.start, parsed_time.time_scope.end],
                )
            )
        elif not parsed_time.time_scope:
            self._apply_default_time_scope(plan, dimension_names)
        if parsed_time.analysis_type:
            plan.analysis_type = parsed_time.analysis_type
        if parsed_time.comparison and parsed_time.comparison.enabled:
            plan.comparison = {
                "enabled": True,
                "mode": parsed_time.comparison.mode,
                "compare_start": parsed_time.comparison.compare_start,
                "compare_end": parsed_time.comparison.compare_end,
                "label": parsed_time.comparison.label,
            }
        if plan.analysis_type == "trend" and "time_month" in dimension_names and "time_month" not in plan.dimensions:
            plan.dimensions.insert(0, "time_month")
        self._apply_trend_order(plan)
        if plan.analysis_type == "ranking" and not plan.order and plan.measures:
            plan.order = [{"member": plan.measures[0], "direction": "DESC"}]
        self._apply_global_topn(question, plan)
        self._normalize_display_dimensions(question, plan, dimension_names)
        self._apply_window_topn(question, plan, dimension_names)
        return plan

    def _normalize_display_dimensions(
        self,
        question: str,
        plan: CubeParsePlan,
        dimension_names: set,
    ) -> None:
        q = question or ""
        if any(k in q for k in ["城市编码", "city_code", "编码"]):
            return
        if "city_name" in dimension_names:
            if "city_code" in plan.dimensions:
                plan.dimensions = [
                    "city_name" if name == "city_code" else name
                    for name in plan.dimensions
                ]
            if any(f.member == "city_code" for f in plan.filters):
                plan.filters = [f for f in plan.filters if f.member != "city_name"]
        seen: set[str] = set()
        plan.dimensions = [
            name for name in plan.dimensions
            if not (name in seen or seen.add(name))
        ]

    def _normalize_name_list(self, raw_items: Any) -> List[str]:
        if not isinstance(raw_items, list):
            raw_items = [raw_items]
        names: List[str] = []
        for item in raw_items:
            if isinstance(item, str):
                name = item.strip()
            elif isinstance(item, dict):
                name = str(
                    item.get("name")
                    or item.get("id")
                    or item.get("member")
                    or item.get("measure")
                    or item.get("dimension")
                    or ""
                ).strip()
            else:
                name = ""
            if name:
                names.append(name)
        seen: set[str] = set()
        result: List[str] = []
        for name in names:
            if name not in seen:
                seen.add(name)
                result.append(name)
        return result

    def _build_parse_prompt(
        self,
        question: str,
        normalized_query: str,
        time_hint: str,
        bundle: CubeBundle,
        rag_examples: List[Dict[str, Any]],
    ) -> str:
        measures = "\n".join(
            f"- {m.measure_name}: {m.title} | {m.description}"
            for m in bundle.measures
            if m.visible
        )
        dimensions = "\n".join(
            f"- {d.dimension_name}: {d.title} | {d.description}"
            for d in bundle.dimensions
            if d.visible
        )
        segments = "\n".join(
            f"- {s.segment_name}: {s.title} | {s.description}"
            for s in bundle.segments
            if s.visible
        )
        rag_text = "\n".join(
            f"- 问题: {item.get('raw_question','')}\n  模板: {item.get('canonical_question','')}\n  SQL: {item.get('sql_text','')[:300]}"
            for item in rag_examples[:3]
        ) or "无"
        time_note = f"已识别时间提示: {time_hint}" if time_hint else "未识别明确时间提示"
        return (
            "你是 Cube 语义层的查询规划器。"
            "请把用户问题解析成 JSON，只能从给定的 measures/dimensions/segments 中选择名字。"
            "不要返回 SQL。"
            "filters 使用格式: {member, operator, values}，operator 可选 equals/in/notIn/contains/between/gte/lte/gt/lt。"
            "如果是“其中 PLUS会员消费了多少”这类问法，请同时保留总指标和子集指标。"
            "如果是“PLUS会员和普通会员对比”，优先用 member_type 分组，而不是 segment。"
            "如果用户只是指定城市/类目/会员类型筛选，不要把它们都当成分组维度。"
            "返回 JSON 字段: measures, dimensions, filters, segments, order, limit, unresolved。\n\n"
            f"measures:\n{measures}\n\n"
            f"dimensions:\n{dimensions}\n\n"
            f"segments:\n{segments}\n\n"
            f"RAG 参考样本:\n{rag_text}\n\n"
            f"{time_note}\n"
            f"原问题: {question}\n"
            f"标准化问题: {normalized_query}"
        )

    def _build_comparison_sql(self, plan: CubeParsePlan) -> str:
        current_filters = [flt for flt in plan.filters if flt.member != "dt"]
        current_filters.append(
            CubeFilter(
                member="dt",
                operator="between",
                values=[plan.time_scope["start"], plan.time_scope["end"]],
            )
        )
        previous_filters = [flt for flt in plan.filters if flt.member != "dt"]
        previous_filters.append(
            CubeFilter(
                member="dt",
                operator="between",
                values=[plan.comparison["compare_start"], plan.comparison["compare_end"]],
            )
        )
        cur_sql = self._cube.generate_sql(
            CubeQuery(
                metrics=plan.measures,
                dimensions=plan.dimensions,
                filters=current_filters,
                segments=plan.segments,
                order=[],
                limit=None,
            )
        )["sql"]
        prev_sql = self._cube.generate_sql(
            CubeQuery(
                metrics=plan.measures,
                dimensions=plan.dimensions,
                filters=previous_filters,
                segments=plan.segments,
                order=[],
                limit=None,
            )
        )["sql"]
        dims = plan.dimensions
        metric = plan.measures[0]
        if dims:
            join_on = " AND ".join([f"cur.{d} = prev.{d}" for d in dims])
            select_dims = ",\n       ".join([f"cur.{d} AS {d}" for d in dims]) + ",\n       "
        else:
            join_on = "1=1"
            select_dims = ""
        order_clause = f"ORDER BY {metric}_mom_rate DESC" if plan.comparison.get("mode") == "mom" else ""
        limit_clause = f"LIMIT {int(plan.limit)}" if plan.limit else ""
        return (
            "WITH cur AS (\n"
            f"{cur_sql}\n"
            "),\nprev AS (\n"
            f"{prev_sql}\n"
            ")\n"
            "SELECT "
            f"{select_dims}cur.{metric} AS {metric},\n"
            f"       prev.{metric} AS {metric}_prev,\n"
            f"       (cur.{metric} - prev.{metric}) AS {metric}_delta,\n"
            f"       (cur.{metric} - prev.{metric}) / NULLIF(prev.{metric}, 0) AS {metric}_mom_rate\n"
            "FROM cur\n"
            f"LEFT JOIN prev ON {join_on}\n"
            f"{order_clause}\n"
            f"{limit_clause}".rstrip()
        )

    def _build_member_compare_sql(self, plan: CubeParsePlan) -> str:
        metric = plan.member_compare.get("base_measure") or (plan.measures[0] if plan.measures else "gmv")
        filters = [
            flt for flt in plan.filters
            if flt.member not in {"member_type", "member_type_code", "is_plus_vip"}
        ]
        cube_result = self._cube.generate_sql(
            CubeQuery(
                metrics=[metric],
                filters=filters,
                derived_metrics=[
                    {
                        "type": "subset",
                        "alias": "member_gmv",
                        "base_measure": metric,
                        "conditions": [
                            {"member": "member_type_code", "operator": "equals", "values": ["1"]}
                        ],
                    },
                    {
                        "type": "subset",
                        "alias": "non_member_gmv",
                        "base_measure": metric,
                        "conditions": [
                            {"member": "member_type_code", "operator": "equals", "values": ["0"]}
                        ],
                    },
                ],
                limit=None,
            )
        )
        base_sql = cube_result["sql"]
        limit_clause = f"LIMIT {int(plan.limit)}" if plan.limit else ""
        return (
            "WITH base AS (\n"
            f"{base_sql}\n"
            ")\n"
            "SELECT\n"
            "  member_gmv,\n"
            "  non_member_gmv,\n"
            "  member_gmv - non_member_gmv AS member_gmv_delta,\n"
            "  member_gmv / NULLIF(non_member_gmv, 0) AS member_gmv_ratio\n"
            "FROM base\n"
            f"{limit_clause}".rstrip()
        )

    def _wrap_window_topn(self, base_sql: str, plan: CubeParsePlan) -> str:
        spec = plan.window_topn or {}
        partition_by = str(spec.get("partition_by") or "").strip()
        order_by = str(spec.get("order_by") or (plan.measures[0] if plan.measures else "")).strip()
        direction = "ASC" if str(spec.get("direction", "DESC")).upper() == "ASC" else "DESC"
        top_n = self._safe_int(spec.get("top_n"), 1) or 1
        if not partition_by or not order_by:
            return base_sql
        select_cols = plan.dimensions + plan.measures
        outer_cols = ",\n  ".join(select_cols)
        template = self._get_cube_template("topn_per_group")
        if template:
            return self._render_sql_template(template, {
                "base_sql": base_sql,
                "partition_by": partition_by,
                "order_by": order_by,
                "direction": direction,
                "top_n": top_n,
                "select_columns": outer_cols,
            })
        return (
            "WITH base AS (\n"
            f"{base_sql}\n"
            "),\nranked AS (\n"
            "SELECT\n"
            "  base.*,\n"
            f"  ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY {order_by} {direction}) AS rn\n"
            "FROM base\n"
            ")\n"
            "SELECT\n"
            f"  {outer_cols}\n"
            "FROM ranked\n"
            f"WHERE rn <= {top_n}\n"
            f"ORDER BY {partition_by} ASC, rn ASC"
        )

    def _with_window_topn_payload(
        self,
        payload: Dict[str, Any],
        window_topn: Dict[str, Any],
    ) -> Dict[str, Any]:
        ordered: Dict[str, Any] = {}
        for key in ("measures", "dimensions", "segments", "filters"):
            if key in payload:
                ordered[key] = payload[key]
        ordered["window_topn"] = window_topn
        for key, value in payload.items():
            if key not in ordered:
                ordered[key] = value
        return ordered

    def _get_cube_template(self, template_name: str) -> str:
        try:
            bundle = self._cube.get_bundle()
            for item in bundle.templates:
                if item.visible and item.template_name == template_name:
                    return item.template_sql or ""
        except Exception as exc:
            logger.warning("[CubePipeline] 加载 SQL 模板失败 template=%s: %s", template_name, exc)
        return ""

    def _render_sql_template(self, template: str, params: Dict[str, Any]) -> str:
        sql = template
        for key, value in params.items():
            sql = sql.replace("{" + key + "}", str(value))
        return sql.strip()

    def _persist_trace(self, trace: RequestTrace) -> None:
        try:
            self._get_trace_db().execute_write(
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
        except Exception as exc:
            logger.warning("[CubePipeline] trace 持久化失败: %s", exc)

    def _fuzzy_resolve_measures(
        self,
        raw_names: List[Any],
        measure_names: set,
    ) -> List[str]:
        """
        当 LLM 返回的 measure 名字不在 catalog 里时，尝试模糊匹配：
        1. 忽略大小写完全匹配
        2. 检查某一方是否是另一方的子串
        3. 按共享 token 数量排序（以 '_' 分割）
        返回最多匹配到的第一个 measure，避免过度猜测。
        """
        resolved: List[str] = []
        for raw in raw_names:
            if not isinstance(raw, str):
                continue
            candidate = raw.strip().lower()
            # pass 1: case-insensitive exact
            for name in measure_names:
                if name.lower() == candidate:
                    resolved.append(name)
                    break
            else:
                # pass 2: substring containment
                best: Optional[str] = None
                best_score = 0
                cand_tokens = set(candidate.split("_"))
                for name in measure_names:
                    nl = name.lower()
                    if candidate in nl or nl in candidate:
                        score = len(min(candidate, nl, key=len))
                        if score > best_score:
                            best_score = score
                            best = name
                    else:
                        # pass 3: shared token overlap
                        name_tokens = set(nl.split("_"))
                        overlap = len(cand_tokens & name_tokens)
                        if overlap > best_score:
                            best_score = overlap
                            best = name
                if best and best_score > 0:
                    resolved.append(best)
        # deduplicate, preserve order
        seen: set = set()
        result: List[str] = []
        for m in resolved:
            if m not in seen:
                seen.add(m)
                result.append(m)
        return result

    def _safe_int(self, value: Any, default: int) -> int:
        try:
            return int(value)
        except Exception:
            return default


def _looks_like_code_value(value: str) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    return bool(re.fullmatch(r"[A-Za-z0-9_-]+", text))
