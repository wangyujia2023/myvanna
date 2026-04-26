"""
SQLGeneratorSemanticAgent：语义路径 SQL 生成 Agent

职责：
  1. 接收 QueryTask + SemanticCatalog，从 catalog 取出 MetricDef / DimensionDef
  2. 对 simple/ratio/derived 指标 → 调用 SQLSynthesizer 模板展开（无 LLM）
  3. 对 high-complexity 指标（如 repurchase_rate）或 unresolved_parts 非空
     → 把 SemanticPlan 作为额外上下文，调用 LLM 生成 SQL（类似旧版路径）
  4. 通过 sql_guard_agent 执行 EXPLAIN 校验
  5. 支持最多 MAX_RETRY 次自修正循环（correction_hint 注入）

这是语义路径的最后一个 Agent，产出可直接执行的 SQL 字符串。
"""
from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from ..semantic.models import IntentPlan, QuerySpec, QueryTask, SemanticPlan
from ..semantic.sql_compiler import SemanticSQLCompiler

if TYPE_CHECKING:
    from ..semantic.catalog import SemanticCatalog
    from ..qwen_client import QwenClient
    from ..agents.sql_guard_agent import SQLGuardAgent

logger = logging.getLogger(__name__)

MAX_RETRY = 3


class SQLGeneratorSemanticAgent:
    """
    语义路径 SQL 生成 Agent。

    Parameters
    ----------
    llm        : QwenClient（高复杂度时使用）
    catalog    : SemanticCatalog
    guard      : SQLGuardAgent（可选，用于 EXPLAIN 校验）
    """

    def __init__(
        self,
        llm: "QwenClient",
        catalog: "SemanticCatalog",
        guard: Optional["SQLGuardAgent"] = None,
    ) -> None:
        self._llm = llm
        self._catalog = catalog
        self._guard = guard
        self._compiler = SemanticSQLCompiler()

    # ─────────────────────────────────────────────────────────────────────────
    # 公共入口
    # ─────────────────────────────────────────────────────────────────────────

    def run(
        self,
        task: QueryTask,
        semantic_plan: SemanticPlan,
        time_range: Optional[Tuple[str, str]] = None,
    ) -> Dict:
        """
        生成 SQL，返回 dict：
        {
            "sql"        : str,
            "path"       : "semantic_template" | "semantic_llm",
            "guard_ok"   : bool,
            "guard_reason": str,
            "attempts"   : int,
        }
        """
        # 判断路径
        use_llm = self._should_use_llm(task, semantic_plan)

        if use_llm:
            return self._llm_path(task, semantic_plan, time_range)
        else:
            return self._template_path(task, semantic_plan, time_range)

    # ─────────────────────────────────────────────────────────────────────────
    # 内部：路径判断
    # ─────────────────────────────────────────────────────────────────────────

    def _should_use_llm(self, task: QueryTask, sp: SemanticPlan) -> bool:
        """
        是否走 LLM 路径：
        - 有 unresolved_parts（某些片段无法映射到 catalog）
        - 任何指标的 complexity == 'high'
        - coverage_score < 0.5（极低匹配度）
        """
        if sp.unresolved_parts:
            logger.info(
                "[SQLGenSemantic] → LLM路径（unresolved_parts=%s）", sp.unresolved_parts
            )
            return True
        if not task.query_spec:
            logger.info("[SQLGenSemantic] → LLM路径（缺少 QuerySpec）")
            return True
        if sp.coverage_score < 0.5:
            logger.info(
                "[SQLGenSemantic] → LLM路径（coverage_score=%.2f < 0.5）", sp.coverage_score
            )
            return True
        for name in task.metrics:
            m = self._catalog.get_metric(name)
            if m and m.is_high_complexity():
                logger.info(
                    "[SQLGenSemantic] → LLM路径（高复杂度指标 %s）", name
                )
                return True
        logger.info(
            "[SQLGenSemantic] → 模板路径（coverage=%.2f，metrics=%s）",
            sp.coverage_score, task.metrics,
        )
        return False

    # ─────────────────────────────────────────────────────────────────────────
    # 内部：模板路径
    # ─────────────────────────────────────────────────────────────────────────

    def _template_path(
        self,
        task: QueryTask,
        sp: SemanticPlan,
        time_range: Optional[Tuple[str, str]],
    ) -> Dict:
        """使用 SQLSynthesizer 确定性生成 SQL，不调用 LLM。"""
        metrics = [self._catalog.get_metric(n) for n in task.metrics]
        missing_metrics = [n for n, m in zip(task.metrics, metrics) if m is None]
        metrics = [m for m in metrics if m is not None]

        dimensions = [self._catalog.get_dimension(n) for n in task.dimensions]
        missing_dims = [n for n, d in zip(task.dimensions, dimensions) if d is None]
        dimensions = [d for d in dimensions if d is not None]

        logger.info(
            "[SQLGenSemantic] 模板路径 | metrics=%s | missing_metrics=%s | "
            "dims=%s | missing_dims=%s | analysis=%s | time_scope=%s | comparison=%s",
            [m.name for m in metrics], missing_metrics,
            [d.name for d in dimensions], missing_dims,
            task.query_spec.analysis_type if task.query_spec else "",
            task.query_spec.time_scope if task.query_spec else None,
            task.query_spec.comparison if task.query_spec else None,
        )

        try:
            query_spec = task.query_spec or QuerySpec(
                metrics=[m.name for m in metrics],
                dimensions=[d.name for d in dimensions],
                filters=list(task.filters),
                order_by=list(task.order_by),
                limit=task.limit,
                unresolved_parts=list(sp.unresolved_parts),
            )
            sql = self._compiler.compile(query_spec, metrics, dimensions)
            logger.info("[SQLGenSemantic] 模板合成成功 | sql=\n%s", sql)
        except Exception as exc:
            logger.error("[SQLGenSemantic] 模板合成失败: %s，回退 LLM 路径", exc)
            return self._llm_path(task, sp, time_range)

        guard_ok, guard_reason = self._guard_check(sql)
        if not guard_ok:
            logger.warning(
                "[SQLGenSemantic] 模板路径 guard FAIL | reason=%s\n--- SQL ---\n%s\n--- END ---",
                guard_reason, sql,
            )

        return {
            "sql": sql,
            "path": "semantic_template_v2",
            "guard_ok": guard_ok,
            "guard_reason": guard_reason,
            "attempts": 1,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # 内部：LLM 路径（含自修正循环）
    # ─────────────────────────────────────────────────────────────────────────

    def _llm_path(
        self,
        task: QueryTask,
        sp: SemanticPlan,
        time_range: Optional[Tuple[str, str]],
    ) -> Dict:
        """调用 LLM 生成 SQL，最多重试 MAX_RETRY 次。"""
        intent = sp.intent_plan
        question = (intent.normalized_query if intent else "") or (
            intent.raw_question if intent else ""
        )

        context = self._build_llm_context(sp, task, time_range)

        logger.info(
            "[SQLGenSemantic] LLM路径启动 | question=%r | metrics=%s | dims=%s | "
            "coverage=%.2f | unresolved=%s | time_range=%s",
            question[:80],
            task.metrics,
            task.dimensions,
            sp.coverage_score,
            sp.unresolved_parts,
            time_range,
        )
        logger.debug("[SQLGenSemantic] LLM context=\n%s", context)

        correction_hint = ""
        last_sql = ""
        guard_ok = False
        guard_reason = ""
        attempt = 0

        for attempt in range(1, MAX_RETRY + 1):
            prompt = self._build_llm_prompt(question, context, correction_hint)

            logger.info(
                "[SQLGenSemantic] attempt=%d | prompt_len=%d",
                attempt, len(prompt),
            )
            logger.debug("[SQLGenSemantic] attempt=%d prompt=\n%s", attempt, prompt)

            try:
                raw = self._llm.generate(prompt, temperature=0.0).strip()
                sql = self._extract_sql(raw)
            except Exception as exc:
                logger.warning(f"[SQLGenSemantic] LLM 调用失败 attempt={attempt}: {exc}")
                sql = last_sql
                break

            last_sql = sql
            logger.info(
                "[SQLGenSemantic] attempt=%d | sql_len=%d | sql_preview=\n%s",
                attempt, len(sql), sql[:600],
            )

            guard_ok, guard_reason = self._guard_check(sql)
            if guard_ok:
                logger.info("[SQLGenSemantic] attempt=%d guard PASS", attempt)
                break

            # 未通过 guard：把错误作为修正提示注入下一轮
            correction_hint = (
                f"上一次生成的 SQL 存在问题：{guard_reason}\n"
                f"出错 SQL：\n{sql}\n"
                f"请修正后只输出 SQL，不要解释。"
            )
            logger.warning(
                "[SQLGenSemantic] attempt=%d guard FAIL | reason=%s\n"
                "--- 出错 SQL ---\n%s\n--- END ---",
                attempt, guard_reason, sql,
            )

        return {
            "sql": last_sql,
            "path": "semantic_llm",
            "guard_ok": guard_ok,
            "guard_reason": guard_reason,
            "attempts": attempt,
        }

    def _build_llm_context(
        self,
        sp: SemanticPlan,
        task: QueryTask,
        time_range: Optional[Tuple[str, str]],
    ) -> str:
        """构建注入 LLM 提示的语义上下文字符串。"""
        lines: List[str] = []

        # 指标详情
        if task.metrics:
            lines.append("【选用指标】")
            for name in task.metrics:
                m = self._catalog.get_metric(name)
                if m:
                    lines.append(
                        f"  {m.name}（{m.label}）: {m.expression}"
                        f"  主表: {m.primary_table} AS {m.primary_alias}"
                    )
                    if m.extra_joins:
                        for j in m.extra_joins:
                            lines.append(f"    JOIN {j.table} {j.alias} ON {j.on}")
                    if m.time_column:
                        lines.append(f"    时间列: {m.time_column}")

        # 维度详情
        if task.dimensions:
            lines.append("【选用维度】")
            for name in task.dimensions:
                d = self._catalog.get_dimension(name)
                if d:
                    lines.append(f"  {d.name}（{d.label}）: {d.expression or d.dim_type}")
                    if d.join:
                        lines.append(
                            f"    JOIN {d.join.table} {d.join.alias} ON {d.join.on}"
                        )

        # 时间范围
        if time_range:
            lines.append(f"【时间范围】{time_range[0]} ~ {time_range[1]}")
        elif sp.intent_plan and sp.intent_plan.time_hint:
            lines.append(f"【时间提示】{sp.intent_plan.time_hint}")

        # 过滤条件
        if task.filters:
            lines.append("【过滤条件】")
            for f in task.filters:
                lines.append(f"  {f.to_sql()}")

        # 未解析片段
        if sp.unresolved_parts:
            lines.append(f"【未映射片段（请自行理解）】{sp.unresolved_parts}")

        return "\n".join(lines)

    def _build_llm_prompt(
        self, question: str, context: str, correction_hint: str
    ) -> str:
        correction_block = ""
        if correction_hint:
            correction_block = (
                f"\n\n【修正提示】上一次生成的 SQL 存在以下问题，请修正：\n{correction_hint}\n"
            )
        return (
            "你是一个精准的 Doris SQL 生成模型。根据以下语义上下文和用户问题，"
            "生成符合 Apache Doris 语法的 SQL。\n"
            "要求：\n"
            "  1. 只输出 SQL，不要解释\n"
            "  2. 使用 WITH...AS 处理复杂子查询\n"
            "  3. 日期使用字符串格式 'YYYY-MM-DD'\n"
            "  4. 所有表必须有别名\n"
            f"{correction_block}\n"
            f"{context}\n\n"
            f"用户问题：{question}\n\n"
            "SQL："
        )

    def _extract_sql(self, raw: str) -> str:
        """从 LLM 输出中提取 SQL（去除 markdown 代码块）。"""
        raw = re.sub(r"^```(?:sql)?\s*", "", raw, flags=re.IGNORECASE)
        raw = re.sub(r"\s*```$", "", raw).strip()
        return raw

    # ─────────────────────────────────────────────────────────────────────────
    # 内部：Guard 校验
    # ─────────────────────────────────────────────────────────────────────────

    def _guard_check(self, sql: str) -> Tuple[bool, str]:
        """调用 SQLGuardAgent 执行 EXPLAIN 校验，Guard 不可用时直接通过。"""
        if not self._guard:
            return True, ""
        try:
            result = self._guard.run(sql)
            ok = result.get("ok", False)
            reason = result.get("reason", "")
            return ok, reason
        except Exception as exc:
            logger.warning(f"[SQLGenSemantic] guard 异常: {exc}")
            return True, ""  # guard 出错不阻断

    # ─────────────────────────────────────────────────────────────────────────
    # 内部：从 SemanticPlan/IntentPlan 推断时间范围
    # ─────────────────────────────────────────────────────────────────────────

    def _extract_time_range(
        self, sp: SemanticPlan
    ) -> Optional[Tuple[str, str]]:
        """
        从 sp.filters 或 intent.time_hint 推断 (start, end) 日期元组。
        返回 None 表示无法推断。
        """
        # 优先从 intent.time_hint 解析，避免被 LLM 自行臆测的时间 filters 干扰
        intent = sp.intent_plan
        if intent and intent.time_hint:
            hint = intent.time_hint
            # "2026-04" → 月份范围
            month_m = re.match(r"^(\d{4})-(\d{2})$", hint)
            if month_m:
                y, mo = int(month_m.group(1)), int(month_m.group(2))
                import calendar
                last_day = calendar.monthrange(y, mo)[1]
                return f"{y}-{mo:02d}-01", f"{y}-{mo:02d}-{last_day:02d}"

            # "2026" → 年度范围
            year_m = re.match(r"^(\d{4})$", hint)
            if year_m:
                y = year_m.group(1)
                return f"{y}-01-01", f"{y}-12-31"

        # 没有可确定 time_hint 时，再从 filters 中找 BETWEEN 时间条件
        for f in sp.filters:
            if f.operator == "BETWEEN" and f.value and f.value2:
                return str(f.value), str(f.value2)

        return None
