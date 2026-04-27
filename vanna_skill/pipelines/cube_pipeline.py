from __future__ import annotations

import concurrent.futures
import json
import logging
import re
import time as _time
from dataclasses import dataclass, field
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
    order: List[Dict[str, str]] = field(default_factory=list)
    limit: Optional[int] = 20
    analysis_type: str = "aggregate"
    unresolved: List[str] = field(default_factory=list)
    comparison: Dict[str, Any] = field(default_factory=dict)
    time_scope: Dict[str, Any] = field(default_factory=dict)


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
                "analysis_type": plan.analysis_type,
                "filters":       [{"member": f.member, "op": f.operator, "values": f.values} for f in plan.filters],
                "time_scope":    plan.time_scope,
                "comparison":    plan.comparison,
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

            if plan.comparison.get("enabled"):
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
                    order=plan.order,
                    limit=plan.limit,
                    rag_hints=[
                        {"question": item.get("raw_question", ""),
                         "sql":      item.get("sql_text", "")}
                        for item in rag_examples[:5]
                    ],
                )
                cube_result       = self._cube.generate_sql(cube_query)
                final_sql         = cube_result["sql"]
                cube_query_payload = cube_result.get("cube_query", {})

            compile_outputs = {
                "path":           "cube_compare" if plan.comparison.get("enabled") else "cube_poc",
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
        if plan.analysis_type == "ranking" and not plan.order and plan.measures:
            plan.order = [{"member": plan.measures[0], "direction": "DESC"}]
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

        if not plan.measures:
            if any(k in q for k in ["净收入", "净营收", "净销售额"]) and "net_revenue" in measure_names:
                plan.measures.append("net_revenue")
            elif any(k in q for k in ["客单价", "笔单价", "AOV", "aov"]) and "aov" in measure_names:
                plan.measures.append("aov")
            elif any(k in q for k in ["PLUS会员消费", "PLUS消费", "plus消费"]) and "plus_consume_amt" in measure_names:
                plan.measures.append("plus_consume_amt")
            elif any(k in q for k in [
                "消费金额", "销售额", "GMV", "gmv", "花了多少钱", "花费",
                "总交易额", "总销售额", "总金额", "交易额", "营业额",
                "收入", "总收入", "总消费",
            ]) and "gmv" in measure_names:
                plan.measures.append("gmv")
            elif any(k in q for k in ["订单数", "成交笔数", "笔数", "单数"]) and "order_count" in measure_names:
                plan.measures.append("order_count")
            elif any(k in q for k in ["销量", "销售量", "件数"]) and "item_sold_qty" in measure_names:
                plan.measures.append("item_sold_qty")
            # last resort: use first visible measure so we don't crash
            elif allow_measure_fallback and measure_names:
                fallback = sorted(measure_names)[0]
                logger.warning("[CubePipeline] measures 为空，兜底使用 %s", fallback)
                plan.measures.append(fallback)

        if not plan.dimensions:
            if any(k in q for k in ["城市", "地区"]) and "city_code" in dimension_names:
                plan.dimensions.append("city_code")
            if any(k in q for k in ["门店类型"]) and "store_type" in dimension_names:
                plan.dimensions.append("store_type")
            if any(k in q for k in ["会员", "PLUS会员", "普通会员"]) and "member_type" in dimension_names and ("对比" in q or "分别" in q):
                plan.dimensions.append("member_type")

        if "PLUS会员" in q and "普通会员" not in q and "plus_members" in segment_names:
            if "plus_members" not in plan.segments:
                plan.segments.append("plus_members")
        if "普通会员" in q and "PLUS会员" not in q and "normal_members" in segment_names:
            if "normal_members" not in plan.segments:
                plan.segments.append("normal_members")

        if "category_1_id" in dimension_names:
            m = re.search(r"一级类目(?:为)?\s*(\d+)", q)
            if m and not any(f.member == "category_1_id" for f in plan.filters):
                plan.filters.append(
                    CubeFilter(member="category_1_id", operator="equals", values=[m.group(1)])
                )

        city_dimension = next((d for d in bundle.dimensions if d.dimension_name == "city_code"), None)
        if city_dimension:
            for city in (city_dimension.enum_mapping or {}).keys():
                if city in q and not any(f.member == "city_code" for f in plan.filters):
                    plan.filters.append(
                        CubeFilter(member="city_code", operator="equals", values=[city])
                    )
                    if "各城市" not in q and "各地区" not in q:
                        plan.dimensions = [d for d in plan.dimensions if d != "city_code"]
                    break

        if "其中" in q and "PLUS会员" in q and "plus_consume_amt" in measure_names and "plus_consume_amt" not in plan.measures:
            plan.measures.append("plus_consume_amt")

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
        if plan.analysis_type == "ranking" and not plan.order and plan.measures:
            plan.order = [{"member": plan.measures[0], "direction": "DESC"}]
        return plan

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
