from __future__ import annotations

import calendar
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Optional

from .models import ComparisonSpec, IntentPlan, QuerySpec, TimeScope

logger = logging.getLogger(__name__)


def _month_bounds(year: int, month: int) -> tuple[str, str]:
    last_day = calendar.monthrange(year, month)[1]
    return f"{year}-{month:02d}-01", f"{year}-{month:02d}-{last_day:02d}"


def _quarter_bounds(year: int, quarter: int) -> tuple[str, str]:
    start_month = (quarter - 1) * 3 + 1
    start = date(year, start_month, 1)
    end_month = start_month + 2
    end = date(year, end_month, calendar.monthrange(year, end_month)[1])
    return start.isoformat(), end.isoformat()


def _shift_year(d: date, years: int) -> date:
    try:
        return d.replace(year=d.year + years)
    except ValueError:
        return d.replace(month=2, day=28, year=d.year + years)


def _parse_iso(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


@dataclass
class TimeParseResult:
    time_scope: Optional[TimeScope]
    comparison: Optional[ComparisonSpec]
    analysis_type: str


class TimeScopeCompiler:
    """
    把自然语言中的时间和对比意图编译成结构化范围。
    只做确定性解析，不依赖 LLM。
    """

    def build(self, question: str, intent: Optional[IntentPlan]) -> TimeParseResult:
        raw = (question or "").strip()
        time_scope = self._resolve_time_scope(raw, intent)
        comparison = self._resolve_comparison(raw, time_scope)
        analysis_type = "compare" if comparison and comparison.enabled else "aggregate"
        if self._is_trend_query(raw):
            analysis_type = "trend"
        if self._is_ranking_query(raw):
            analysis_type = "ranking" if analysis_type == "aggregate" else analysis_type
        return TimeParseResult(
            time_scope=time_scope,
            comparison=comparison,
            analysis_type=analysis_type,
        )

    def _resolve_time_scope(
        self, question: str, intent: Optional[IntentPlan]
    ) -> Optional[TimeScope]:
        hint = (intent.time_hint if intent else "") or ""
        today = datetime.now().date()
        if hint:
            if re.match(r"^\d{4}-\d{2}$", hint):
                y, m = map(int, hint.split("-"))
                start, end = _month_bounds(y, m)
                return TimeScope("month", "day", start, end, hint, hint)
            if re.match(r"^\d{4}$", hint):
                return TimeScope("year", "day", f"{hint}-01-01", f"{hint}-12-31", hint, hint)
            if hint == "今天":
                d = today.isoformat()
                return TimeScope("day", "day", d, d, hint, hint)
            if hint == "昨天":
                d = (today - timedelta(days=1)).isoformat()
                return TimeScope("day", "day", d, d, hint, hint)
            if hint in ("本月", "当月"):
                start, end = _month_bounds(today.year, today.month)
                return TimeScope("month", "day", start, end, hint, hint)
            if hint in ("上月", "上个月"):
                ref = (today.replace(day=1) - timedelta(days=1))
                start, end = _month_bounds(ref.year, ref.month)
                return TimeScope("month", "day", start, end, hint, hint)
            if hint in ("本年", "今年"):
                return TimeScope("year", "day", f"{today.year}-01-01", f"{today.year}-12-31", hint, hint)
            if hint in ("本季度", "当季"):
                quarter = (today.month - 1) // 3 + 1
                start, end = _quarter_bounds(today.year, quarter)
                return TimeScope("quarter", "day", start, end, hint, hint)
            if hint == "上季度":
                this_q = (today.month - 1) // 3 + 1
                if this_q == 1:
                    year, quarter = today.year - 1, 4
                else:
                    year, quarter = today.year, this_q - 1
                start, end = _quarter_bounds(year, quarter)
                return TimeScope("quarter", "day", start, end, hint, hint)

        if "昨天" in question or "昨日" in question:
            d = (today - timedelta(days=1)).isoformat()
            return TimeScope("day", "day", d, d, "昨日", "昨日")
        if "今天" in question or "今日" in question:
            d = today.isoformat()
            return TimeScope("day", "day", d, d, "今日", "今日")

        range_m = re.search(r"(\d{4}-\d{2}-\d{2})\s*(?:到|至|-)\s*(\d{4}-\d{2}-\d{2})", question)
        if range_m:
            return TimeScope(
                "range", "day", range_m.group(1), range_m.group(2),
                f"{range_m.group(1)}~{range_m.group(2)}", range_m.group(0),
            )
        return None

    def _resolve_comparison(
        self, question: str, time_scope: Optional[TimeScope]
    ) -> Optional[ComparisonSpec]:
        if not time_scope:
            return None
        q = question or ""
        if any(k in q for k in ["同比", "去年同期"]):
            return self._build_yoy(time_scope)
        if any(k in q for k in ["环比", "较上期", "上一期", "前一周期"]):
            return self._build_previous_period(time_scope, "mom")
        if any(k in q for k in ["周同比", "周环比", "较上周"]):
            return self._build_previous_period(time_scope, "wow")
        return None

    def _build_yoy(self, time_scope: TimeScope) -> ComparisonSpec:
        start = _shift_year(_parse_iso(time_scope.start), -1).isoformat()
        end = _shift_year(_parse_iso(time_scope.end), -1).isoformat()
        return ComparisonSpec(
            mode="yoy",
            enabled=True,
            compare_start=start,
            compare_end=end,
            output_style="both",
            label="同比",
        )

    def _build_previous_period(self, time_scope: TimeScope, mode: str) -> ComparisonSpec:
        start = _parse_iso(time_scope.start)
        end = _parse_iso(time_scope.end)
        delta_days = (end - start).days + 1
        prev_end = start - timedelta(days=1)
        prev_start = prev_end - timedelta(days=delta_days - 1)
        return ComparisonSpec(
            mode=mode,
            enabled=True,
            compare_start=prev_start.isoformat(),
            compare_end=prev_end.isoformat(),
            output_style="both",
            label="环比" if mode == "mom" else "对比",
        )

    def _is_trend_query(self, question: str) -> bool:
        return any(k in question for k in ["趋势", "变化", "走势", "按月", "每日", "每天", "各月"])

    def _is_ranking_query(self, question: str) -> bool:
        return any(k in question for k in ["排名", "top", "TOP", "最高", "最低", "前十", "前10"])


def enrich_query_spec(question: str, intent: Optional[IntentPlan], spec: QuerySpec) -> QuerySpec:
    parsed = TimeScopeCompiler().build(question, intent)
    spec.time_scope = parsed.time_scope
    spec.comparison = parsed.comparison
    spec.analysis_type = parsed.analysis_type
    if spec.analysis_type == "ranking" and not spec.order_by and spec.metrics:
        spec.order_by = [{"field": spec.metrics[0], "direction": "DESC"}]
        spec.limit = spec.limit or 20
    if spec.comparison and spec.comparison.enabled and spec.limit is None:
        spec.limit = 20
    return spec
