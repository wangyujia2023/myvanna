from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ..core.security import assert_readonly_sql
from ..doris_client import DorisClient
from ..cube.service import CubeFilter, CubeQuery, CubeService

logger = logging.getLogger(__name__)


@dataclass
class RCARequest:
    metric: str
    time_dimension: str
    current_start: str
    current_end: str
    baseline_start: str
    baseline_end: str
    dimensions: List[str] = field(default_factory=list)
    filters: List[CubeFilter] = field(default_factory=list)
    limit: int = 20


class RCAService:
    """归因分析 MVP：基于 Cube SQL 的维度贡献分解。

    第一版聚焦可控性：所有 SQL 都由 Cube 生成，执行前走只读校验；
    结果侧计算当前期、对比期、差值、贡献率，为后续 ReAct/Plan-Act 提供工具层。
    """

    def __init__(self, config: dict, cube_service: CubeService):
        self._config = config
        self._cube = cube_service
        self._biz = DorisClient(
            host=config["host"],
            port=config["port"],
            user=config["user"],
            password=config.get("password", ""),
            database=config.get("database", "retail_dw"),
        )

    def options(self) -> Dict[str, Any]:
        bundle = self._cube.get_bundle()
        measures = [
            {
                "name": item.measure_name,
                "title": item.title,
                "cube_name": item.cube_name,
                "description": item.description,
                "format": item.format,
            }
            for item in bundle.measures
            if item.visible
        ]
        dimensions = self._dedupe_dimensions(bundle.dimensions)
        influences = self._load_influences()
        return {
            "measures": measures,
            "dimensions": dimensions,
            "time_dimensions": [item for item in dimensions if item["type"] == "time"],
            "influences": influences,
        }

    def _dedupe_dimensions(self, dimensions: List[Any]) -> List[Dict[str, Any]]:
        seen = set()
        result = []
        for item in dimensions:
            if not item.visible or item.dimension_name in seen:
                continue
            seen.add(item.dimension_name)
            result.append(
                {
                    "name": item.dimension_name,
                    "title": item.title,
                    "cube_name": item.cube_name,
                    "type": item.dimension_type,
                    "description": item.description,
                }
            )
        return result

    def analyze(self, req: RCARequest) -> Dict[str, Any]:
        limit = max(1, min(int(req.limit or 20), 100))
        current_filters = [*req.filters, self._time_filter(req, "current")]
        baseline_filters = [*req.filters, self._time_filter(req, "baseline")]

        current_total = self._run_metric(req.metric, current_filters)
        baseline_total = self._run_metric(req.metric, baseline_filters)
        total_delta = current_total - baseline_total

        dimension_results = []
        sql_trace = []
        for dim in req.dimensions[:8]:
            current_rows, current_sql = self._run_dimension(req.metric, dim, current_filters, limit)
            baseline_rows, baseline_sql = self._run_dimension(req.metric, dim, baseline_filters, limit)
            sql_trace.append({"dimension": dim, "period": "current", "sql": current_sql})
            sql_trace.append({"dimension": dim, "period": "baseline", "sql": baseline_sql})
            dimension_results.append(
                {
                    "dimension": dim,
                    "items": self._merge_dimension_rows(
                        dim,
                        req.metric,
                        current_rows,
                        baseline_rows,
                        total_delta,
                        limit,
                    ),
                }
            )

        return {
            "status": "ok",
            "metric": req.metric,
            "time_dimension": req.time_dimension,
            "periods": {
                "current": {"start": req.current_start, "end": req.current_end, "value": current_total},
                "baseline": {"start": req.baseline_start, "end": req.baseline_end, "value": baseline_total},
            },
            "delta": total_delta,
            "delta_rate": self._safe_div(total_delta, baseline_total),
            "dimensions": dimension_results,
            "sql_trace": sql_trace,
            "summary": self._summary(req.metric, current_total, baseline_total, total_delta, dimension_results),
        }

    def _run_metric(self, metric: str, filters: List[CubeFilter]) -> float:
        result = self._cube.generate_sql(CubeQuery(metrics=[metric], filters=filters, limit=1))
        sql = result["sql"]
        assert_readonly_sql(sql)
        logger.info("[RCA] total SQL metric=%s\n%s", metric, sql)
        rows = self._biz.execute(sql)
        if not rows:
            return 0.0
        return self._num(rows[0].get(metric))

    def _run_dimension(
        self,
        metric: str,
        dimension: str,
        filters: List[CubeFilter],
        limit: int,
    ) -> tuple[List[dict], str]:
        result = self._cube.generate_sql(
            CubeQuery(
                metrics=[metric],
                dimensions=[dimension],
                filters=filters,
                order=[{"member": metric, "direction": "desc"}],
                limit=limit,
            )
        )
        sql = result["sql"]
        assert_readonly_sql(sql)
        logger.info("[RCA] dimension SQL metric=%s dimension=%s\n%s", metric, dimension, sql)
        return self._biz.execute(sql), sql

    def _time_filter(self, req: RCARequest, period: str) -> CubeFilter:
        if period == "current":
            values = [req.current_start, req.current_end]
        else:
            values = [req.baseline_start, req.baseline_end]
        return CubeFilter(member=req.time_dimension, operator="between", values=values)

    def _merge_dimension_rows(
        self,
        dimension: str,
        metric: str,
        current_rows: List[dict],
        baseline_rows: List[dict],
        total_delta: float,
        limit: int,
    ) -> List[dict]:
        current = {str(row.get(dimension)): self._num(row.get(metric)) for row in current_rows}
        baseline = {str(row.get(dimension)): self._num(row.get(metric)) for row in baseline_rows}
        keys = set(current) | set(baseline)
        items = []
        for key in keys:
            cur = current.get(key, 0.0)
            base = baseline.get(key, 0.0)
            delta = cur - base
            items.append(
                {
                    "value": key,
                    "current": cur,
                    "baseline": base,
                    "delta": delta,
                    "delta_rate": self._safe_div(delta, base),
                    "contribution": self._safe_div(delta, total_delta),
                    "direction": "up" if delta > 0 else "down" if delta < 0 else "flat",
                }
            )
        items.sort(key=lambda item: abs(item["delta"]), reverse=True)
        return items[:limit]

    def _summary(
        self,
        metric: str,
        current_total: float,
        baseline_total: float,
        total_delta: float,
        dimension_results: List[dict],
    ) -> str:
        direction = "上升" if total_delta > 0 else "下降" if total_delta < 0 else "持平"
        top_parts = []
        for dim in dimension_results:
            top = (dim.get("items") or [])[:3]
            if not top:
                continue
            desc = "、".join(f"{item['value']}({item['delta']:.2f})" for item in top)
            top_parts.append(f"{dim['dimension']} 主要贡献：{desc}")
        suffix = "；".join(top_parts) if top_parts else "暂无显著维度贡献。"
        return (
            f"{metric} 当前期 {current_total:.2f}，对比期 {baseline_total:.2f}，"
            f"变化 {total_delta:.2f}，整体{direction}。{suffix}"
        )

    def _load_influences(self) -> List[Dict[str, Any]]:
        try:
            return self._cube._client.execute(
                """
                SELECT source_metric, target_metric, relation_type, weight, direction, description
                FROM cube_store.cube_metric_influences
                WHERE visible = 1
                ORDER BY target_metric, ABS(weight) DESC
                LIMIT 500
                """
            )
        except Exception as exc:
            logger.warning("[RCA] 加载指标影响关系失败: %s", exc)
            return []

    def _num(self, value: Any) -> float:
        if value is None:
            return 0.0
        try:
            return float(value)
        except Exception:
            return 0.0

    def _safe_div(self, numerator: float, denominator: float) -> Optional[float]:
        if denominator in (0, 0.0):
            return None
        return numerator / denominator
