"""
QueryPlanAgent：查询计划 Agent

职责：
  把 SemanticPlan 拆解成可执行的 QueryPlan（DAG）。

Phase 1 实现：
  - 简单/复合查询 → 单 task（t1），sequential 模式
  - high-complexity 指标（如 repurchase_rate）→ 单 task，标记 task_type=sql_query

Phase 2 预留：
  - attribution 查询 → task_type=attribution 任务（当前生成占位 task）
  - 并行子任务（parallel 模式）

设计思路：
  QueryPlanAgent 本身 **不调用 LLM**，是纯规则拆解；
  如果 SemanticPlan.complexity == 'attribution'（Phase 2），
  才会生成 attribution 类型 task 并标记 execution_mode=parallel。
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List, Optional

from ..semantic.models import (
    FilterCondition,
    IntentPlan,
    QueryPlan,
    QueryTask,
    SemanticPlan,
)

if TYPE_CHECKING:
    from ..semantic.catalog import SemanticCatalog

logger = logging.getLogger(__name__)


class QueryPlanAgent:
    """
    规则式查询计划生成，不调用 LLM。

    Parameters
    ----------
    catalog : SemanticCatalog，用于判断指标复杂度
    """

    def __init__(self, catalog: Optional["SemanticCatalog"] = None) -> None:
        self._catalog = catalog

    # ─────────────────────────────────────────────────────────────────────────
    # 公共入口
    # ─────────────────────────────────────────────────────────────────────────

    def run(self, semantic_plan: SemanticPlan) -> QueryPlan:
        """
        输入 SemanticPlan，输出 QueryPlan。
        Phase 1：始终返回单 task 计划。
        Phase 2：attribution intent → 多 task DAG（预留占位）。
        """
        intent = semantic_plan.intent_plan

        # Phase 2 attribution 预留
        if intent and intent.complexity == "attribution":
            return self._build_attribution_plan(semantic_plan)

        # 标准路径：单 sql_query task
        return self._build_single_task_plan(semantic_plan)

    # ─────────────────────────────────────────────────────────────────────────
    # 内部：单 task 计划
    # ─────────────────────────────────────────────────────────────────────────

    def _build_single_task_plan(self, sp: SemanticPlan) -> QueryPlan:
        """生成单个 sql_query 任务的计划。"""
        # 将 SemanticPlan 的 filters 直接透传
        task = QueryTask(
            task_id="t1",
            task_type=self._infer_task_type(sp),
            metrics=list(sp.metrics),
            dimensions=list(sp.dimensions),
            filters=list(sp.filters),
            order_by=list(sp.order_by),
            limit=sp.limit or 20,
            query_spec=sp.query_spec,
            depends_on=[],
            description=self._describe(sp),
        )

        return QueryPlan(
            tasks=[task],
            execution_mode="sequential",
            semantic_plan=sp,
        )

    def _describe(self, sp: SemanticPlan) -> str:
        """生成简单的 task 描述文字（供调试/日志）。"""
        intent = sp.intent_plan
        question = (intent.normalized_query if intent else "") or ""
        metrics_str = ", ".join(sp.metrics) if sp.metrics else "（无指标）"
        dims_str = ", ".join(sp.dimensions) if sp.dimensions else "（无维度）"
        return f"查询 {metrics_str} 按 {dims_str}" + (f" | {question[:40]}" if question else "")

    def _infer_task_type(self, sp: SemanticPlan) -> str:
        spec = sp.query_spec
        if spec and spec.comparison and spec.comparison.enabled:
            return "compare"
        if spec and spec.analysis_type == "trend":
            return "trend"
        if spec and spec.analysis_type == "ranking":
            return "ranking"
        return "sql_query"

    # ─────────────────────────────────────────────────────────────────────────
    # 内部：attribution 计划（Phase 2 预留）
    # ─────────────────────────────────────────────────────────────────────────

    def _build_attribution_plan(self, sp: SemanticPlan) -> QueryPlan:
        """
        Phase 2 预留：attribution 多任务 DAG。
        当前返回两个任务：
          t1 - 主指标时序数据（sql_query）
          t2 - 归因分析（attribution，依赖 t1）
        """
        intent = sp.intent_plan
        question = (intent.normalized_query if intent else "") or ""
        logger.info(f"[QueryPlanAgent] attribution plan: {question[:60]!r}")

        t1 = QueryTask(
            task_id="t1",
            task_type="sql_query",
            metrics=list(sp.metrics),
            dimensions=list(sp.dimensions),
            filters=list(sp.filters),
            order_by=list(sp.order_by),
            limit=sp.limit or 20,
            depends_on=[],
            description=f"主指标时序数据 | {question[:40]}",
        )

        # Phase 2：attribution task 执行器尚未实现，此处只生成计划结构
        t2 = QueryTask(
            task_id="t2",
            task_type="attribution",          # Phase 2 执行器处理
            metrics=list(sp.metrics),
            dimensions=[],
            filters=list(sp.filters),
            order_by=[],
            limit=0,
            depends_on=["t1"],
            description=f"归因分析（Phase 2 预留）| {question[:40]}",
        )

        return QueryPlan(
            tasks=[t1, t2],
            execution_mode="sequential",      # Phase 2 改为 parallel
            semantic_plan=sp,
        )
