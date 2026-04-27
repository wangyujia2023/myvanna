"""
SQLSynthesizer：模板展开式 SQL 合成（非 LLM）

核心流程：
  1. 从 SemanticCatalog 取出每个 MetricDef / DimensionDef
  2. 合并主表和所有 JOIN
  3. 展开 SELECT 列（指标聚合 + 维度字段）
  4. 拼装 WHERE / GROUP BY / ORDER BY / LIMIT
  5. 返回 SQL 字符串

不调用任何 LLM，输出确定性 SQL，用于语义路径主干。
"""
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Set, Tuple

from .models import (
    DimensionDef,
    FilterCondition,
    JoinDef,
    MetricDef,
    QueryTask,
)

logger = logging.getLogger(__name__)


class SQLSynthesizer:
    """
    无状态 SQL 合成器，每次调用 synthesize() 生成一条 SQL。
    不持有 catalog 引用——调用方把所需的 MetricDef / DimensionDef 传进来。
    """

    # ─────────────────────────────────────────────────────────────────────────
    # 公共入口
    # ─────────────────────────────────────────────────────────────────────────

    def synthesize(
        self,
        task: QueryTask,
        metrics: List[MetricDef],
        dimensions: List[DimensionDef],
        time_range: Optional[Tuple[str, str]] = None,
        *,
        dialect: str = "doris",          # doris | mysql（保留扩展点）
    ) -> str:
        """
        从 QueryTask + Def 列表生成 SQL。

        Parameters
        ----------
        task        : 包含 filters / order_by / limit 的查询任务
        metrics     : 需要出现在 SELECT 的指标定义列表
        dimensions  : 需要出现在 SELECT 的维度定义列表
        time_range  : (start, end) 日期字符串，如 ("2026-01-01", "2026-01-31")
                      不提供时使用 task.filters 里的时间条件
        dialect     : SQL 方言（当前仅 doris，预留）

        Returns
        -------
        str  格式化好的 SQL 字符串
        """
        # ── 特殊查询模式路由 ─────────────────────────────────────────────────
        for m in metrics:
            if m.query_pattern == "topn_per_group":
                logger.debug(
                    "[SQLSynthesizer] topn_per_group 路径: metric=%s params=%s",
                    m.name, m.template_params,
                )
                return self._synthesize_topn_per_group(
                    task, metrics, dimensions, time_range
                )
        # ── 普通聚合路径 ─────────────────────────────────────────────────────
        ctx = self._build_context(task, metrics, dimensions, time_range)
        return self._render(ctx)

    # ─────────────────────────────────────────────────────────────────────────
    # 内部：构建中间上下文
    # ─────────────────────────────────────────────────────────────────────────

    def _build_context(
        self,
        task: QueryTask,
        metrics: List[MetricDef],
        dimensions: List[DimensionDef],
        time_range: Optional[Tuple[str, str]],
    ) -> Dict:
        """把所有输入整理成一个渲染字典。"""

        # 1. 确定主表（取第一个有 primary_source 的指标）
        primary_table, primary_alias = self._pick_primary_source(metrics)

        # 2. 收集所有 JOIN（指标 extra_joins + 维度 join）
        joins: List[JoinDef] = []
        seen_aliases: Set[str] = {primary_alias}
        joins = self._collect_joins(metrics, dimensions, seen_aliases, primary_alias)

        # 3. 确定时间列（取第一个指标的 time_column），并替换 {alias} 占位符
        time_col = next((m.time_column for m in metrics if m.time_column), "")
        if time_col:
            time_col = time_col.replace("{alias}", primary_alias)

        # 4. 构建 SELECT 列表
        select_cols = self._build_select(metrics, dimensions, primary_alias, time_col)

        # 5. 构建 WHERE 子句
        where_clauses = self._build_where(
            task.filters, time_col, time_range, primary_alias
        )

        # 6. 构建 GROUP BY
        group_by_cols = self._build_group_by(dimensions)

        # 7. ORDER BY
        order_by_cols = self._build_order_by(task.order_by)

        return {
            "primary_table": primary_table,
            "primary_alias": primary_alias,
            "joins": joins,
            "select_cols": select_cols,
            "where_clauses": where_clauses,
            "group_by_cols": group_by_cols,
            "order_by_cols": order_by_cols,
            "limit": None if (not group_by_cols and not order_by_cols) else (task.limit or 20),
        }

    # ── 主表选取 ──────────────────────────────────────────────────────────────

    def _pick_primary_source(
        self, metrics: List[MetricDef]
    ) -> Tuple[str, str]:
        """从指标列表中选出主表名和别名。"""
        for m in metrics:
            if m.primary_source:
                return m.primary_table, m.primary_alias
        # 兜底：没有任何指标有 primary_source（极少数场景）
        return "dual", "t"

    # ── JOIN 收集 ─────────────────────────────────────────────────────────────

    def _collect_joins(
        self,
        metrics: List[MetricDef],
        dimensions: List[DimensionDef],
        seen: Set[str],
        fact_alias: str,
    ) -> List[JoinDef]:
        """
        收集去重后的 JOIN 列表。
        去重条件：alias 相同则视为同一个 JOIN，跳过后续重复项。
        """
        joins: List[JoinDef] = []

        # 指标的 extra_joins
        for m in metrics:
            for j in m.extra_joins:
                if j.alias not in seen:
                    seen.add(j.alias)
                    # 替换 on 条件中的 {fact_alias} 占位符
                    j_copy = JoinDef(
                        table=j.table,
                        alias=j.alias,
                        join_type=j.join_type,
                        on=j.on.replace("{fact_alias}", fact_alias),
                    )
                    joins.append(j_copy)

        # 维度的 join
        for d in dimensions:
            if d.join and d.join.alias not in seen:
                seen.add(d.join.alias)
                j = d.join
                j_copy = JoinDef(
                    table=j.table,
                    alias=j.alias,
                    join_type=j.join_type,
                    on=j.on.replace("{fact_alias}", fact_alias),
                )
                joins.append(j_copy)

        return joins

    # ── SELECT ────────────────────────────────────────────────────────────────

    def _build_select(
        self,
        metrics: List[MetricDef],
        dimensions: List[DimensionDef],
        fact_alias: str,
        time_col: str = "",
    ) -> List[str]:
        """
        构建 SELECT 列列表（字符串），维度在前，指标在后。
        """
        cols: List[str] = []

        # 维度列
        for d in dimensions:
            cols.extend(self._dimension_select_cols(d, fact_alias, time_col))

        # 指标列
        for m in metrics:
            expr = self._resolve_metric_expr(m, fact_alias)
            cols.append(f"{expr} AS {m.name}")

        return cols

    def _dimension_select_cols(
        self, dim: DimensionDef, fact_alias: str, time_col: str = ""
    ) -> List[str]:
        """把单个维度展开成若干 SELECT 列。"""
        cols: List[str] = []
        alias = dim.select_alias  # dim.alias or dim.name

        if dim.dim_type == "time":
            # 时间维度：展开表达式，如 DATE_FORMAT(o.dt, '%Y-%m') AS stat_month
            # time_col 优先使用来自指标定义的值（已含表别名，如 o.dt）；
            # 若 time_col 不含 "."，则补充 fact_alias 前缀；兜底使用 fact_alias.dt
            if time_col:
                resolved_time_col = time_col if "." in time_col else f"{fact_alias}.{time_col}"
            else:
                resolved_time_col = f"{fact_alias}.dt"
            expr = dim.expression.replace("{time_col}", resolved_time_col)
            cols.append(f"{expr} AS {alias}")

        elif dim.dim_type == "entity_ref":
            # 实体引用维度：从 join 表取字段
            if dim.join and dim.select_fields:
                j_alias = dim.join.alias
                for field in dim.select_fields:
                    # field 可能含别名：store_id, store_name AS sname
                    cols.append(f"{j_alias}.{field}")
            elif dim.join:
                # 无 select_fields，至少取一个字段
                cols.append(f"{dim.join.alias}.*")

        else:
            # attribute 维度：直接取 fact 表字段
            # expression 例如 "o.category_id"，没有就用 "{fact_alias}.{name}"
            if dim.expression:
                expr = dim.expression.replace("{fact_alias}", fact_alias)
                cols.append(f"{expr} AS {alias}")
            else:
                cols.append(f"{fact_alias}.{dim.name} AS {alias}")

        return cols

    def _resolve_metric_expr(self, metric: MetricDef, fact_alias: str) -> str:
        """
        解析指标的聚合表达式。
        ratio 类型优先用 numerator/denominator 拼装。
        所有表达式中的 {alias} 占位符均替换为 fact_alias。
        """
        if metric.metric_type == "ratio":
            num = (metric.numerator_expr or "1").replace("{alias}", fact_alias)
            den = (metric.denominator_expr or "1").replace("{alias}", fact_alias)
            # 避免除零
            return (
                f"ROUND(SUM({num}) / NULLIF(SUM({den}), 0), 4)"
            )
        # simple / derived / composite：直接使用 expression
        expr = metric.expression
        if not expr:
            expr = "COUNT(*)"
        # 替换 {alias} 占位符为实际主表别名（如 o / t / f）
        expr = expr.replace("{alias}", fact_alias)
        return expr

    # ── WHERE ────────────────────────────────────────────────────────────────

    def _build_where(
        self,
        filters: List[FilterCondition],
        time_col: str,
        time_range: Optional[Tuple[str, str]],
        fact_alias: str,
    ) -> List[str]:
        """构建 WHERE 子句列表（每项是一个条件字符串）。"""
        clauses: List[str] = []

        # 1. 时间范围条件（优先级高于 filters 里的时间条件）
        if time_range and time_col:
            start, end = time_range
            col = time_col if "." in time_col else f"{fact_alias}.{time_col}"
            clauses.append(f"{col} BETWEEN '{start}' AND '{end}'")

        # 2. 其余 filter 条件
        for f in filters:
            # 如果是时间列且已有 time_range，跳过避免重复
            if time_range and time_col and f.column in (time_col, time_col.split(".")[-1]):
                continue
            clauses.append(f.to_sql())

        return clauses

    # ── GROUP BY ─────────────────────────────────────────────────────────────

    def _build_group_by(self, dimensions: List[DimensionDef]) -> List[str]:
        """构建 GROUP BY 列列表。"""
        cols: List[str] = []
        for d in dimensions:
            if d.dim_type == "time":
                cols.append(d.select_alias)
            elif d.dim_type == "entity_ref":
                if d.join and d.select_fields:
                    j_alias = d.join.alias
                    for sf in d.select_fields:
                        # 取别名或原字段名
                        col_name = sf.split(" AS ")[-1].strip() if " AS " in sf else f"{j_alias}.{sf}"
                        cols.append(col_name)
                elif d.join:
                    pass  # entity_ref 无 select_fields 时不加 group by
            else:
                alias = d.select_alias
                cols.append(alias)
        return cols

    # ── ORDER BY ─────────────────────────────────────────────────────────────

    def _build_order_by(
        self, order_by: List[Dict]
    ) -> List[str]:
        """把 [{"field": "net_revenue", "direction": "DESC"}] 转为字符串列表。"""
        cols: List[str] = []
        for item in order_by:
            field = item.get("field", "")
            direction = item.get("direction", "DESC").upper()
            if field:
                cols.append(f"{field} {direction}")
        return cols

    # ─────────────────────────────────────────────────────────────────────────
    # 特殊模式：TopN per group（窗口函数子查询）
    # ─────────────────────────────────────────────────────────────────────────

    def _synthesize_topn_per_group(
        self,
        task: QueryTask,
        metrics: List[MetricDef],
        dimensions: List[DimensionDef],
        time_range: Optional[Tuple[str, str]],
    ) -> str:
        """
        生成 ROW_NUMBER() OVER (PARTITION BY …) 形式的 TopN 子查询 SQL。

        模板结构：
          SELECT {outer_cols}
          FROM (
            SELECT {inner_cols},
                   {metric_expr} AS {metric_name},
                   ROW_NUMBER() OVER (
                     PARTITION BY {partition_alias}
                     ORDER BY {metric_expr} {order_dir}
                   ) AS rk
            FROM   {primary_table} {primary_alias}
            {join_clauses}
            WHERE  …
            GROUP BY {inner_group_by}
          ) _ranked
          WHERE  rk <= {top_n}
          ORDER BY {partition_alias}, rk
        """
        # 取出第一个 topn_per_group 指标（通常只有一个）
        metric = next(m for m in metrics if m.query_pattern == "topn_per_group")
        params = metric.template_params or {}

        primary_alias = metric.primary_alias
        primary_table = metric.primary_table
        top_n = task.limit or params.get("top_n", 3)
        order_dir = params.get("order_dir", "DESC").upper()
        partition_dim_name = params.get("partition_dim", "")

        # 1. 收集 JOINs（去重）
        seen: Set[str] = {primary_alias}
        joins = self._collect_joins(metrics, dimensions, seen, primary_alias)

        # 2. 时间列
        time_col = metric.time_column.replace("{alias}", primary_alias)

        # 3. 找到 partition 维度 & 其余维度（group 维度）
        partition_dim = next(
            (d for d in dimensions if d.name == partition_dim_name), None
        )
        group_dims = [d for d in dimensions if d.name != partition_dim_name]

        # 4. 构建 partition SELECT 列（内层 + 外层共用别名）
        if partition_dim:
            part_cols = self._dimension_select_cols(partition_dim, primary_alias, time_col)
            partition_select_expr = part_cols[0] if part_cols else f"{primary_alias}.{partition_dim_name}"
            partition_alias = partition_dim.select_alias
        else:
            # partition_dim 不在传入列表中，从 metric.template_params 推断
            partition_select_expr = f"{primary_alias}.{partition_dim_name} AS {partition_dim_name}"
            partition_alias = partition_dim_name

        # 5. 构建 group 维度列（内层）
        inner_dim_cols: List[str] = [partition_select_expr]
        inner_group_by: List[str] = [partition_alias]

        for d in group_dims:
            cols = self._dimension_select_cols(d, primary_alias, time_col)
            inner_dim_cols.extend(cols)
            # GROUP BY 引用
            for col in cols:
                if " AS " in col.upper():
                    gb_col = col.rsplit(" AS ", 1)[-1].strip()
                elif "." in col:
                    gb_col = col.strip()
                else:
                    gb_col = col.strip()
                inner_group_by.append(gb_col)

        # 6. 指标表达式
        metric_expr = self._resolve_metric_expr(metric, primary_alias)
        metric_name = metric.name

        # 7. WHERE 子句
        where_clauses = self._build_where(task.filters, time_col, time_range, primary_alias)

        # 8. 拼装 JOIN 块
        join_lines = "\n  ".join(
            f"{j.join_type} {j.table} {j.alias} ON {j.on}" for j in joins
        )

        # 9. WHERE 块
        if where_clauses:
            where_block = "WHERE  " + "\n     AND ".join(where_clauses)
        else:
            where_block = ""

        # 10. 内层 SELECT 的最后两列：聚合指标 + 窗口函数
        inner_metric_col = f"{metric_expr} AS {metric_name}"
        inner_rn_col = (
            f"ROW_NUMBER() OVER (\n"
            f"           PARTITION BY {partition_alias}\n"
            f"           ORDER BY {metric_expr} {order_dir}\n"
            f"         ) AS rk"
        )

        inner_select_parts = inner_dim_cols + [inner_metric_col, inner_rn_col]
        inner_select_str = ",\n         ".join(inner_select_parts)
        inner_group_str = ", ".join(inner_group_by)

        # 11. 外层 SELECT：解析所有别名
        outer_cols: List[str] = []
        for col in inner_dim_cols:
            if " AS " in col.upper():
                outer_cols.append(col.rsplit(" AS ", 1)[-1].strip())
            elif "." in col:
                outer_cols.append(col.split(".")[-1].strip())
            else:
                outer_cols.append(col.strip())
        outer_cols.append(metric_name)
        outer_cols.append("rk")
        outer_select_str = ", ".join(outer_cols)

        # 12. 拼装完整 SQL
        lines: List[str] = [f"SELECT {outer_select_str}"]
        lines.append("FROM (")
        lines.append(f"  SELECT {inner_select_str}")
        lines.append(f"  FROM   {primary_table} {primary_alias}")
        if join_lines:
            lines.append(f"  {join_lines}")
        if where_block:
            lines.append(f"  {where_block}")
        if inner_group_str:
            lines.append(f"  GROUP BY {inner_group_str}")
        lines.append(") _ranked")
        lines.append(f"WHERE  rk <= {top_n}")
        lines.append(f"ORDER BY {partition_alias}, rk")

        return "\n".join(lines)

    # ─────────────────────────────────────────────────────────────────────────
    # 内部：渲染 SQL 字符串
    # ─────────────────────────────────────────────────────────────────────────

    def _render(self, ctx: Dict) -> str:
        """把上下文字典渲染成最终 SQL。"""
        lines: List[str] = []

        # SELECT
        select_part = ",\n       ".join(ctx["select_cols"]) if ctx["select_cols"] else "1"
        lines.append(f"SELECT {select_part}")

        # FROM
        lines.append(f"FROM   {ctx['primary_table']} {ctx['primary_alias']}")

        # JOINs
        for j in ctx["joins"]:
            lines.append(f"{j.join_type} {j.table} {j.alias} ON {j.on}")

        # WHERE
        if ctx["where_clauses"]:
            where_str = "\n   AND ".join(ctx["where_clauses"])
            lines.append(f"WHERE  {where_str}")

        # GROUP BY
        if ctx["group_by_cols"]:
            group_str = ", ".join(ctx["group_by_cols"])
            lines.append(f"GROUP BY {group_str}")

        # ORDER BY
        if ctx["order_by_cols"]:
            order_str = ", ".join(ctx["order_by_cols"])
            lines.append(f"ORDER BY {order_str}")

        # LIMIT
        if ctx["limit"]:
            lines.append(f"LIMIT  {ctx['limit']}")

        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# 便捷函数
# ─────────────────────────────────────────────────────────────────────────────

_synthesizer = SQLSynthesizer()


def synthesize_sql(
    task: QueryTask,
    metrics: List[MetricDef],
    dimensions: List[DimensionDef],
    time_range: Optional[Tuple[str, str]] = None,
) -> str:
    """模块级便捷调用，使用全局 SQLSynthesizer 单例。"""
    return _synthesizer.synthesize(task, metrics, dimensions, time_range)
