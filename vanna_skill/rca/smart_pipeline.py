from __future__ import annotations

import json
import logging
import queue
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from hashlib import md5
from typing import Any, Callable, Dict, List, Optional

from ..cube.service import CubeService
from ..doris_client import DorisClient
from ..qwen_client import QwenClient
from .service import RCARequest, RCAService

logger = logging.getLogger(__name__)


class SmartRCAPipeline:
    """对话式智能归因编排。

    第一版保留清晰的多路召回结构：
    - Cube 指标/维度召回
    - 指标影响关系召回
    - LLM 归因参数规划
    - RCA 工具执行
    """

    def __init__(self, config: dict, cube_service: CubeService, rca_service: RCAService):
        self._config = config
        self._cube = cube_service
        self._rca = rca_service
        self._llm = QwenClient(
            api_key=config.get("qwen_api_key", ""),
            model=config.get("model", "qwen-plus"),
        )
        self._store_client: Optional[DorisClient] = None

    def stream(self, question: str):
        events: queue.Queue = queue.Queue()

        def emit(event_type: str, payload: dict):
            events.put((event_type, payload))

        def worker():
            try:
                self.run(question, emit)
            except Exception as exc:
                logger.exception("[SmartRCA] failed: %s", exc)
                emit("error", {"error": str(exc)})

        threading.Thread(target=worker, daemon=True).start()
        while True:
            try:
                event_type, payload = events.get(timeout=60)
                yield event_type, payload
                if event_type in ("final", "error"):
                    break
            except queue.Empty:
                yield "keepalive", {}

    def run(self, question: str, emit: Optional[Callable[[str, dict], None]] = None) -> dict:
        trace_id = uuid.uuid4().hex[:8]
        started = time.time()
        steps: List[dict] = []

        def notify(event_type: str, data: dict):
            if emit:
                emit(event_type, data)

        def step(name: str, label: str, fn):
            notify("step_start", {"name": name, "label": label})
            t0 = time.time()
            try:
                outputs = fn()
                item = {
                    "name": name,
                    "status": "ok",
                    "duration_ms": round((time.time() - t0) * 1000, 1),
                    "outputs": outputs or {},
                }
            except Exception as exc:
                item = {
                    "name": name,
                    "status": "error",
                    "duration_ms": round((time.time() - t0) * 1000, 1),
                    "outputs": {},
                    "error": str(exc),
                }
                steps.append(item)
                notify("step_done", item)
                raise
            steps.append(item)
            notify("step_done", item)
            return outputs

        notify("start", {"trace_id": trace_id, "question": question})
        plan: dict = {}
        result_holder: dict = {}
        try:
            intent = step("rca_intent", "① 归因意图识别", lambda: self._intent(question))

            def recalls():
                with ThreadPoolExecutor(max_workers=2) as pool:
                    f_options = pool.submit(self._options_recall, question)
                    f_influences = pool.submit(self._influence_recall, question)
                    return f_options.result(), f_influences.result()

            recall_context = step(
                "rca_multi_recall",
                "② 多路 Agent 召回",
                lambda: self._pack_recalls(*recalls()),
            )

            plan_pack = step(
                "rca_plan",
                "③ 归因计划生成",
                lambda: self._plan(question, intent, recall_context, recall_context),
            )
            plan = plan_pack["normalized_plan"]

            def execute():
                req = RCARequest(
                    metric=plan["metric"],
                    time_dimension=plan["time_dimension"],
                    current_start=plan["current_start"],
                    current_end=plan["current_end"],
                    baseline_start=plan["baseline_start"],
                    baseline_end=plan["baseline_end"],
                    dimensions=plan["dimensions"],
                    limit=int(plan.get("limit") or 20),
                )
                result_holder.update(self._rca.analyze(req))
                top_contributors = self._top_contributors(result_holder)
                return {
                    "metric": req.metric,
                    "time_dimension": req.time_dimension,
                    "dimensions": req.dimensions,
                    "sql_count": len(result_holder.get("sql_trace", [])),
                    "sql_preview": [
                        {
                            "dimension": item.get("dimension"),
                            "period": item.get("period"),
                            "sql": (item.get("sql") or "")[:800],
                        }
                        for item in result_holder.get("sql_trace", [])[:8]
                    ],
                    "top_contributors": top_contributors[:10],
                    "delta": result_holder.get("delta"),
                    "delta_rate": result_holder.get("delta_rate"),
                }

            step("rca_execute", "④ RCA 工具执行", execute)

            summary = step(
                "rca_summary",
                "⑤ 归因结论生成",
                lambda: {"summary": result_holder.get("summary", "")},
            )

            trace = {
                "trace_id": trace_id,
                "question": question,
                "status": "ok",
                "total_ms": round((time.time() - started) * 1000, 1),
                "steps": steps,
            }
            final = {
                "trace": trace,
                "question": question,
                "plan": plan,
                "result": result_holder,
                "summary": summary.get("summary", ""),
            }
            self._persist_run(trace_id, question, "ok", plan, result_holder, final["summary"], steps)
            notify("final", final)
            return final
        except Exception as exc:
            trace = {
                "trace_id": trace_id,
                "question": question,
                "status": "error",
                "total_ms": round((time.time() - started) * 1000, 1),
                "steps": steps,
                "error": str(exc),
            }
            self._persist_run(trace_id, question, "error", plan, result_holder, str(exc), steps)
            raise

    def _top_contributors(self, result_holder: dict) -> List[dict]:
        top_contributors = []
        for dim in result_holder.get("dimensions", []):
            for item in (dim.get("items") or [])[:3]:
                top_contributors.append(
                    {
                        "dimension": dim.get("dimension"),
                        "value": item.get("value"),
                        "delta": item.get("delta"),
                        "contribution": item.get("contribution"),
                        "explanatory_power": item.get("explanatory_power"),
                        "surprise": item.get("surprise"),
                        "adtributor_score": item.get("adtributor_score"),
                    }
                )
        top_contributors.sort(key=lambda item: abs(float(item.get("contribution") or 0)), reverse=True)
        return top_contributors

    def _get_store_client(self) -> DorisClient:
        if self._store_client is None:
            self._store_client = DorisClient(
                host=self._config["host"],
                port=int(self._config["port"]),
                user=self._config["user"],
                password=self._config.get("password", ""),
                database=self._config.get("rca_store_database", "rca_store"),
            )
        return self._store_client

    def _persist_run(
        self,
        run_id: str,
        question: str,
        status: str,
        plan: dict,
        result: dict,
        report_text: str,
        steps: List[dict],
    ) -> None:
        try:
            db = _safe_identifier(self._config.get("rca_store_database", "rca_store"), "rca_store")
            payload_plan = {**(plan or {}), "steps": steps}
            candidates = self._top_contributors(result)
            self._get_store_client().execute_write(
                f"""
                INSERT INTO {db}.rca_runs
                  (run_id, question, metric_name, status, plan_json,
                   candidates_json, causal_results_json, report_text)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    question,
                    (plan or {}).get("metric") or (result or {}).get("metric") or "",
                    status,
                    json.dumps(payload_plan, ensure_ascii=False),
                    json.dumps(candidates[:50], ensure_ascii=False),
                    json.dumps(result or {}, ensure_ascii=False),
                    report_text or "",
                ),
            )
            for rank, item in enumerate(candidates[:50], 1):
                self._get_store_client().execute_write(
                    f"""
                    INSERT INTO {db}.rca_run_candidates
                      (candidate_id, run_id, rank_no, candidate_type, candidate_json,
                       runtime_contribution, prior_score, causal_score, final_score)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        _stable_bigint(run_id, rank, item.get("dimension"), item.get("value")),
                        run_id,
                        rank,
                        "dimension_value",
                        json.dumps(item, ensure_ascii=False),
                        item.get("contribution"),
                        item.get("adtributor_score"),
                        None,
                        item.get("adtributor_score"),
                    ),
                )
        except Exception as exc:
            logger.warning("[SmartRCA] 持久化运行记录失败: %s", exc)

    def _intent(self, question: str) -> dict:
        return {
            "intent_type": "root_cause",
            "normalized_query": question.strip(),
            "analysis_mode": "metric_attribution",
            "signals": [item for item in ["归因", "原因", "波动", "下滑", "增长", "异动"] if item in question],
        }

    def _options_recall(self, question: str) -> dict:
        options = self._rca.options()
        measures = options.get("measures", [])
        dimensions = options.get("dimensions", [])
        return {
            "measures": measures,
            "dimensions": dimensions,
            "time_dimensions": options.get("time_dimensions", []),
            "measure_count": len(measures),
            "dimension_count": len(dimensions),
            "time_dimension_count": len(options.get("time_dimensions", [])),
            "candidate_measures": [
                {
                    "name": item.get("name"),
                    "title": item.get("title"),
                    "cube_name": item.get("cube_name"),
                    "format": item.get("format"),
                }
                for item in measures[:20]
            ],
            "candidate_dimensions": [
                {
                    "name": item.get("name"),
                    "title": item.get("title"),
                    "cube_name": item.get("cube_name"),
                    "type": item.get("type"),
                }
                for item in dimensions[:30]
            ],
        }

    def _influence_recall(self, question: str) -> dict:
        options = self._rca.options()
        influences = options.get("influences", [])
        return {
            "influences": influences,
            "influence_count": len(influences),
            "candidate_influences": [
                {
                    "source_node": item.get("source_node"),
                    "source_title": item.get("source_title"),
                    "target_node": item.get("target_node"),
                    "target_title": item.get("target_title"),
                    "edge_type": item.get("edge_type"),
                    "prior_strength": item.get("prior_strength"),
                    "confidence": item.get("confidence"),
                    "prior_score": item.get("prior_score"),
                    "direction": item.get("direction"),
                    "lag": item.get("lag"),
                }
                for item in influences[:20]
            ],
            "rca_graph": options.get("rca_graph", {}),
        }

    def _pack_recalls(self, options: dict, influences: dict) -> dict:
        return {
            **options,
            **influences,
            "top_measures": [m["name"] for m in options.get("measures", [])[:8]],
            "top_dimensions": [d["name"] for d in options.get("dimensions", [])[:12]],
        }

    def _plan(self, question: str, intent: dict, options: dict, influences: dict) -> dict:
        measures = options.get("measures", [])
        dimensions = options.get("dimensions", [])
        time_dimensions = options.get("time_dimensions", [])
        prompt = self._build_plan_prompt(question, measures, dimensions, time_dimensions, influences.get("influences", []))
        data, raw = self._llm_json(prompt)
        normalized = self._normalize_plan(data, measures, dimensions, time_dimensions)
        return {
            **normalized,
            "normalized_plan": normalized,
            "raw_plan": data,
            "raw_response": raw[:3000],
            "prompt_chars": len(prompt),
            "available_measure_count": len(measures),
            "available_dimension_count": len(dimensions),
            "normalization_notes": self._normalization_notes(data, normalized),
        }

    def _build_plan_prompt(
        self,
        question: str,
        measures: List[dict],
        dimensions: List[dict],
        time_dimensions: List[dict],
        influences: List[dict],
    ) -> str:
        return (
            "你是智能归因分析规划 Agent。请根据用户问题，从可用 Metric Cube 配置中选择归因参数。"
            "只返回 JSON，不要解释。\n"
            "JSON 字段：metric, time_dimension, current_start, current_end, baseline_start, baseline_end, dimensions, limit。\n"
            "时间如果问题没有明确说明，默认 current 为 2026-04-01 到 2026-04-30，baseline 为 2026-03-01 到 2026-03-31。\n"
            "dimensions 选择 1-5 个最适合归因的非时间维度。\n\n"
            f"用户问题：{question}\n\n"
            f"可用指标：{json.dumps(measures[:80], ensure_ascii=False)}\n\n"
            f"可用时间维度：{json.dumps(time_dimensions[:20], ensure_ascii=False)}\n\n"
            f"可用维度：{json.dumps(dimensions[:120], ensure_ascii=False)}\n\n"
            f"指标影响关系：{json.dumps(influences[:80], ensure_ascii=False)}"
        )

    def _llm_json(self, prompt: str) -> tuple[dict, str]:
        raw = self._llm.generate(prompt, temperature=0.0).strip()
        clean = re.sub(r"^```(?:json)?\s*", "", raw)
        clean = re.sub(r"\s*```$", "", clean).strip()
        return json.loads(clean), raw

    def _normalization_notes(self, raw: dict, normalized: dict) -> List[str]:
        notes = []
        for key in ["metric", "time_dimension", "current_start", "current_end", "baseline_start", "baseline_end"]:
            if str(raw.get(key) or "") != str(normalized.get(key) or ""):
                notes.append(f"{key}: {raw.get(key)!r} -> {normalized.get(key)!r}")
        raw_dims = raw.get("dimensions") if isinstance(raw.get("dimensions"), list) else []
        if raw_dims != normalized.get("dimensions"):
            notes.append(f"dimensions: {raw_dims!r} -> {normalized.get('dimensions')!r}")
        return notes

    def _normalize_plan(
        self,
        data: dict,
        measures: List[dict],
        dimensions: List[dict],
        time_dimensions: List[dict],
    ) -> dict:
        measure_names = {m["name"] for m in measures}
        dim_names = {d["name"] for d in dimensions}
        time_names = {d["name"] for d in time_dimensions}
        metric = data.get("metric")
        if metric not in measure_names:
            metric = "gmv" if "gmv" in measure_names else next(iter(measure_names), "")
        time_dimension = data.get("time_dimension")
        if time_dimension not in time_names:
            time_dimension = "dt" if "dt" in time_names else next(iter(time_names), "dt")
        dims = [d for d in data.get("dimensions", []) if d in dim_names and d not in time_names]
        if not dims:
            preferred = ["city_code", "store_type", "category_1_id", "member_type", "store_id"]
            dims = [d for d in preferred if d in dim_names][:3]
        return {
            "metric": metric,
            "time_dimension": time_dimension,
            "current_start": str(data.get("current_start") or "2026-04-01"),
            "current_end": str(data.get("current_end") or "2026-04-30"),
            "baseline_start": str(data.get("baseline_start") or "2026-03-01"),
            "baseline_end": str(data.get("baseline_end") or "2026-03-31"),
            "dimensions": dims[:5],
            "limit": int(data.get("limit") or 20),
        }


def _stable_bigint(*parts: Any) -> int:
    raw = "\n".join(str(part or "") for part in parts)
    return int(md5(raw.encode("utf-8")).hexdigest()[:15], 16)


def _safe_identifier(value: str, default: str) -> str:
    text = str(value or default).strip() or default
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", text):
        raise ValueError(f"非法数据库名: {text}")
    return text
