from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from .models import ComparisonSpec, DimensionDef, FilterCondition, JoinDef, MetricDef, QuerySpec

logger = logging.getLogger(__name__)


@dataclass
class SourceGroup:
    key: str
    primary_table: str
    primary_alias: str
    time_column: str
    metrics: List[MetricDef] = field(default_factory=list)
    dimensions: List[DimensionDef] = field(default_factory=list)
    joins: List[JoinDef] = field(default_factory=list)


class SemanticSQLCompiler:
    """
    确定性 SQL 编译器。
    把 QuerySpec + MetricDef/DimensionDef 编译成聚合、趋势、对比 SQL。
    """

    def compile(
        self,
        spec: QuerySpec,
        metrics: List[MetricDef],
        dimensions: List[DimensionDef],
    ) -> str:
        if not metrics:
            raise ValueError("缺少指标，无法编译 SQL")
        groups = self._build_source_groups(metrics, dimensions)
        if spec.comparison and spec.comparison.enabled:
            return self._compile_compare(spec, groups, dimensions)
        return self._compile_aggregate(spec, groups, dimensions)

    def _build_source_groups(
        self, metrics: List[MetricDef], dimensions: List[DimensionDef]
    ) -> List[SourceGroup]:
        groups: Dict[str, SourceGroup] = {}
        for metric in metrics:
            primary_table = metric.primary_table or "dual"
            primary_alias = metric.primary_alias or "t"
            time_column = metric.time_column.replace("{alias}", primary_alias) if metric.time_column else ""
            joins = self._collect_joins(metric, dimensions, primary_alias)
            key = self._group_key(primary_table, primary_alias, time_column, joins)
            group = groups.setdefault(
                key,
                SourceGroup(
                    key=key,
                    primary_table=primary_table,
                    primary_alias=primary_alias,
                    time_column=time_column,
                    joins=joins,
                ),
            )
            group.metrics.append(metric)
            for dim in dimensions:
                if dim not in group.dimensions:
                    group.dimensions.append(dim)
        return list(groups.values())

    def _collect_joins(
        self, metric: MetricDef, dimensions: List[DimensionDef], fact_alias: str
    ) -> List[JoinDef]:
        joins: List[JoinDef] = []
        seen = set()
        for j in metric.extra_joins:
            on = (j.on or "").replace("{fact_alias}", fact_alias)
            key = (j.table, j.alias, j.join_type, on)
            if key in seen:
                continue
            seen.add(key)
            joins.append(JoinDef(j.table, j.alias, j.join_type, on))
        for dim in dimensions:
            if dim.join:
                j = dim.join
                on = (j.on or "").replace("{fact_alias}", fact_alias)
                key = (j.table, j.alias, j.join_type, on)
                if key in seen:
                    continue
                seen.add(key)
                joins.append(JoinDef(j.table, j.alias, j.join_type, on))
        return joins

    def _group_key(
        self, primary_table: str, primary_alias: str, time_column: str, joins: List[JoinDef]
    ) -> str:
        joins_key = "|".join(f"{j.join_type}:{j.table}:{j.alias}:{j.on}" for j in joins)
        return f"{primary_table}|{primary_alias}|{time_column}|{joins_key}"

    def _compile_aggregate(
        self, spec: QuerySpec, groups: List[SourceGroup], dimensions: List[DimensionDef]
    ) -> str:
        if len(groups) == 1:
            return self._render_single_group_select(
                groups[0], spec, dimensions, compare=None, cte_name=None
            )
        ctes = []
        final_select = []
        join_lines = []
        dim_selects = self._dimension_projection(groups[0], dimensions)
        current_aliases = []
        for idx, group in enumerate(groups, start=1):
            cte_name = f"src_{idx}"
            ctes.append(
                f"{cte_name} AS (\n{self._indent(self._render_single_group_select(group, spec, dimensions, compare=None, cte_name=cte_name), 2)}\n)"
            )
            alias = f"s{idx}"
            current_aliases.append(alias)
            if idx == 1:
                for expr, alias_name in dim_selects:
                    final_select.append(f"{alias}.{alias_name} AS {alias_name}")
                for metric in group.metrics:
                    final_select.append(f"{alias}.{metric.name} AS {metric.name}")
            else:
                join_condition = " AND ".join(
                    f"s1.{alias_name} = {alias}.{alias_name}" for _, alias_name in dim_selects
                ) or "1=1"
                join_lines.append(f"LEFT JOIN {cte_name} {alias} ON {join_condition}")
                for metric in group.metrics:
                    final_select.append(f"{alias}.{metric.name} AS {metric.name}")
        lines = [f"WITH\n{',\n'.join(ctes)}", "SELECT " + ",\n       ".join(final_select), f"FROM   src_1 s1"]
        lines.extend(join_lines)
        if spec.order_by:
            lines.append("ORDER BY " + ", ".join(self._render_order_by(spec.order_by)))
        if spec.limit:
            lines.append(f"LIMIT  {spec.limit}")
        return "\n".join(lines)

    def _compile_compare(
        self, spec: QuerySpec, groups: List[SourceGroup], dimensions: List[DimensionDef]
    ) -> str:
        comparison = spec.comparison
        if not comparison:
            raise ValueError("缺少 comparison 定义")
        if len(groups) > 1:
            raise ValueError("当前版本暂不支持跨多个主源表的对比编译")
        group = groups[0]
        cur_sql = self._render_single_group_select(group, spec, dimensions, compare=None, cte_name="cur")
        prev_sql = self._render_single_group_select(group, spec, dimensions, compare=comparison, cte_name="prev")
        dim_selects = self._dimension_projection(group, dimensions)
        select_cols: List[str] = []
        join_keys: List[str] = []
        for _, alias_name in dim_selects:
            select_cols.append(f"cur.{alias_name} AS {alias_name}")
            join_keys.append(f"cur.{alias_name} = prev.{alias_name}")
        first_rate = ""
        for metric in group.metrics:
            curr_alias = metric.name
            prev_alias = f"{metric.name}_prev"
            rate_alias = f"{metric.name}_{comparison.mode}_rate"
            delta_alias = f"{metric.name}_{comparison.mode}_delta"
            select_cols.append(f"cur.{curr_alias} AS {curr_alias}")
            select_cols.append(f"prev.{curr_alias} AS {prev_alias}")
            select_cols.append(f"(cur.{curr_alias} - prev.{curr_alias}) AS {delta_alias}")
            select_cols.append(
                f"(cur.{curr_alias} - prev.{curr_alias}) / NULLIF(prev.{curr_alias}, 0) AS {rate_alias}"
            )
            first_rate = first_rate or rate_alias
        lines = [
            "WITH",
            f"cur AS (\n{self._indent(cur_sql, 2)}\n),",
            f"prev AS (\n{self._indent(prev_sql, 2)}\n)",
            "SELECT " + ",\n       ".join(select_cols),
            "FROM   cur",
            f"LEFT JOIN prev ON {' AND '.join(join_keys) if join_keys else '1=1'}",
        ]
        if spec.order_by:
            lines.append("ORDER BY " + ", ".join(self._render_order_by(spec.order_by)))
        elif first_rate:
            lines.append(f"ORDER BY {first_rate} DESC")
        if spec.limit:
            lines.append(f"LIMIT  {spec.limit}")
        return "\n".join(lines)

    def _render_single_group_select(
        self,
        group: SourceGroup,
        spec: QuerySpec,
        dimensions: List[DimensionDef],
        compare: Optional[ComparisonSpec],
        cte_name: Optional[str],
    ) -> str:
        time_scope = spec.time_scope
        select_cols = []
        group_by = []
        for expr, alias_name in self._dimension_projection(group, dimensions):
            select_cols.append(f"{expr} AS {alias_name}")
            group_by.append(alias_name)
        for metric in group.metrics:
            select_cols.append(f"{self._metric_expr(metric, group.primary_alias)} AS {metric.name}")
        where_clauses = self._render_time_clause(
            group.time_column,
            compare.compare_start if compare else (time_scope.start if time_scope else ""),
            compare.compare_end if compare else (time_scope.end if time_scope else ""),
            group.primary_alias,
        )
        where_clauses.extend(self._render_filters(spec.filters, group, dimensions))
        lines = [
            "SELECT " + ",\n       ".join(select_cols),
            f"FROM   {group.primary_table} {group.primary_alias}",
        ]
        for j in group.joins:
            lines.append(f"{j.join_type} {j.table} {j.alias} ON {j.on}")
        if where_clauses:
            lines.append("WHERE  " + "\n   AND ".join(where_clauses))
        if group_by:
            lines.append("GROUP BY " + ", ".join(group_by))
        return "\n".join(lines)

    def _dimension_projection(
        self, group: SourceGroup, dimensions: List[DimensionDef]
    ) -> List[Tuple[str, str]]:
        cols: List[Tuple[str, str]] = []
        for dim in dimensions:
            alias_name = dim.select_alias

            if dim.dim_type == "time":
                time_col = group.time_column if "." in group.time_column else f"{group.primary_alias}.{group.time_column or 'dt'}"
                expr = (dim.expression or "{time_col}").replace("{time_col}", time_col)
                cols.append((expr, alias_name))

            elif dim.dim_type == "entity_ref" and dim.join:
                cols.extend(self._entity_ref_projection(group, dim, alias_name))
            else:
                expr = dim.expression.replace("{fact_alias}", group.primary_alias) if dim.expression else f"{group.primary_alias}.{dim.name}"
                cols.append((expr, alias_name))

        return cols

    def _entity_ref_projection(
        self,
        group: SourceGroup,
        dim: DimensionDef,
        alias_name: str,
    ) -> List[Tuple[str, str]]:
        """
        entity_ref 维度投影规范：
        1. 有 expression 时优先使用 expression，适合 CASE WHEN / 业务映射
        2. 没 expression 时再展开 select_fields
        3. 都没有时兜底为 join_alias.name
        """
        join_alias = dim.join.alias if dim.join else ""
        if dim.expression:
            expr = dim.expression.replace("{fact_alias}", group.primary_alias)
            expr = expr.replace("{join_alias}", join_alias)
            return [(expr, alias_name)]

        if dim.select_fields:
            cols: List[Tuple[str, str]] = []
            for field in dim.select_fields:
                cols.append(self._normalize_select_field(join_alias, field))
            return cols

        return [(f"{join_alias}.{dim.name}", alias_name)]

    def _normalize_select_field(self, join_alias: str, field: str) -> Tuple[str, str]:
        """
        统一处理 select_fields:
        - `store_type`
        - `s_dim.store_type`
        - `store_type AS type_name`
        - `s_dim.store_type AS type_name`
        """
        field = (field or "").strip()
        if not field:
            return (f"{join_alias}.*", "*")
        if re.search(r"\s+AS\s+", field, flags=re.IGNORECASE):
            expr, alias = re.split(r"\s+AS\s+", field, maxsplit=1, flags=re.IGNORECASE)
            expr = expr.strip()
            alias = alias.strip()
            if "." not in expr:
                expr = f"{join_alias}.{expr}"
            return (expr, alias)
        if "." in field:
            return (field, field.split(".")[-1])
        return (f"{join_alias}.{field}", field)

    def _metric_expr(self, metric: MetricDef, fact_alias: str) -> str:
        if metric.metric_type == "ratio":
            num = (metric.numerator_expr or "1").replace("{alias}", fact_alias)
            den = (metric.denominator_expr or "1").replace("{alias}", fact_alias)
            return f"ROUND(SUM({num}) / NULLIF(SUM({den}), 0), 4)"
        expr = metric.expression or "COUNT(*)"
        return expr.replace("{alias}", fact_alias)

    def _render_time_clause(
        self, time_column: str, start: str, end: str, fact_alias: str
    ) -> List[str]:
        if not (time_column and start and end):
            return []
        col = time_column if "." in time_column else f"{fact_alias}.{time_column}"
        if start == end:
            return [f"{col} = '{start}'"]
        return [f"{col} BETWEEN '{start}' AND '{end}'"]

    def _render_filters(
        self,
        filters: List[FilterCondition],
        group: SourceGroup,
        dimensions: List[DimensionDef],
    ) -> List[str]:
        clauses: List[str] = []
        dimension_map = {d.name: d for d in dimensions}
        alias_map = {d.select_alias: d for d in dimensions}
        for flt in filters:
            col = (flt.column or "").strip()
            if not col or col.lower() in {"time", "date", "日期", "时间"}:
                continue
            if group.time_column:
                time_leaf = group.time_column.split(".")[-1]
                if col in {group.time_column, time_leaf}:
                    continue
            resolved = self._resolve_filter_column(col, dimension_map, alias_map, group)
            if not resolved:
                logger.warning("[SemanticSQLCompiler] 跳过无法解析的过滤列: %s", col)
                continue
            clauses.append(self._render_filter_sql(flt, resolved))
        return clauses

    def _resolve_filter_column(
        self,
        column: str,
        dimension_map: Dict[str, DimensionDef],
        alias_map: Dict[str, DimensionDef],
        group: SourceGroup,
    ) -> str:
        dim = dimension_map.get(column) or alias_map.get(column)
        if dim:
            if dim.dim_type == "entity_ref" and dim.join and dim.select_fields:
                return f"{dim.join.alias}.{dim.select_fields[0].split(' AS ')[0].strip()}"
            if dim.expression:
                return dim.expression.replace("{fact_alias}", group.primary_alias)
            return f"{group.primary_alias}.{dim.name}"
        if "." in column:
            return column
        return f"{group.primary_alias}.{column}"

    def _render_filter_sql(self, flt: FilterCondition, column_expr: str) -> str:
        if flt.operator == "BETWEEN" and flt.value2 is not None:
            return f"{column_expr} BETWEEN '{flt.value}' AND '{flt.value2}'"
        if flt.operator == "IN" and isinstance(flt.value, list):
            vals = ", ".join(f"'{v}'" for v in flt.value)
            return f"{column_expr} IN ({vals})"
        if flt.operator == "LIKE":
            return f"{column_expr} LIKE '%{flt.value}%'"
        return f"{column_expr} {flt.operator} '{flt.value}'"

    def _render_order_by(self, items: List[Dict[str, str]]) -> List[str]:
        rendered: List[str] = []
        for item in items:
            field = (item.get("field") or "").strip()
            direction = (item.get("direction") or "DESC").upper()
            if field:
                rendered.append(f"{field} {direction}")
        return rendered

    def _indent(self, text: str, spaces: int) -> str:
        prefix = " " * spaces
        return "\n".join(prefix + line if line else line for line in text.splitlines())
