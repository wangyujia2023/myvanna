from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ..config_store import ROOT_DIR
from ..doris_client import DorisClient
from .models import CubeBundle, CubeDimension, CubeJoin, CubeMeasure, CubeSegment
from .renderer import CubeModelRenderer
from .store import CubeStoreRepository

logger = logging.getLogger(__name__)
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_CUBE_REF_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_\.]*)\}")


@dataclass
class CubeFilter:
    member: str
    operator: str = "equals"
    values: List[Any] = field(default_factory=list)


@dataclass
class CubeQuery:
    metrics: List[str] = field(default_factory=list)
    dimensions: List[str] = field(default_factory=list)
    filters: List[CubeFilter] = field(default_factory=list)
    segments: List[str] = field(default_factory=list)
    order: List[Dict[str, str]] = field(default_factory=list)
    limit: Optional[int] = None
    rag_hints: List[Dict[str, Any]] = field(default_factory=list)


class CubeService:
    """
    Cube 适配服务：
    - 从 cube_store 读取配置
    - 渲染为 Cube 原生模型文件
    - 提供一个 PoC 级 SQL 生成器，便于先联调现有链路
    """

    def __init__(self, config: dict):
        self._config = config
        cube_db = config.get("cube_store_database", "cube_store")
        self._client = DorisClient(
            host=config["host"],
            port=config["port"],
            user=config["user"],
            password=config.get("password", ""),
            database=cube_db,
        )
        self._repo = CubeStoreRepository(self._client)
        target_dir = ROOT_DIR / config.get("cube_generated_dir", "cube/model/generated")
        self._renderer = CubeModelRenderer(target_dir)
        self._last_manifest: Dict[str, Any] = {}
        self._last_bundle: Optional[CubeBundle] = None
        self._reload_each_request = bool(config.get("cube_model_reload_each_request", False))

    def get_model_status(self) -> Dict[str, Any]:
        if not self._reload_each_request and self._last_manifest:
            return {
                "database": self._config.get("cube_store_database", "cube_store"),
                "generated_dir": str(self._renderer._target_dir),
                "load_mode": "memory",
                "active_version": self._last_manifest.get("version_no", 0),
                "active_checksum": self._last_manifest.get("checksum", ""),
                "rendered_version": self._last_manifest.get("version_no", 0),
                "rendered_checksum": self._last_manifest.get("checksum", ""),
                "files": self._last_manifest.get("files", []),
            }
        latest = self._repo.get_latest_version()
        return {
            "database": self._config.get("cube_store_database", "cube_store"),
            "generated_dir": str(self._renderer._target_dir),
            "load_mode": "db_each_request" if self._reload_each_request else "memory",
            "active_version": int(latest.get("version_no", 0) or 0),
            "active_checksum": latest.get("checksum", ""),
            "rendered_version": self._last_manifest.get("version_no", 0),
            "rendered_checksum": self._last_manifest.get("checksum", ""),
            "files": self._last_manifest.get("files", []),
        }

    def get_bundle(self) -> CubeBundle:
        if self._last_bundle is None:
            self.ensure_models()
        return self._last_bundle or self._repo.load_bundle()

    def ensure_models(self, force: bool = False) -> Dict[str, Any]:
        if not force and not self._reload_each_request and self._last_manifest:
            return self._last_manifest

        latest = self._repo.get_latest_version()
        latest_version = int(latest.get("version_no", 0) or 0)
        latest_checksum = latest.get("checksum", "")
        if (
            not force
            and not self._reload_each_request
            and self._last_manifest
            and self._last_manifest.get("version_no") == latest_version
        ):
            return self._last_manifest

        bundle = self._repo.load_bundle()
        self._last_bundle = bundle
        self._last_manifest = self._renderer.render_bundle(bundle)
        logger.info(
            "[CubeService] rendered bundle version=%s checksum=%s load_mode=%s files=%s",
            bundle.version_no,
            bundle.checksum,
            "db_each_request" if self._reload_each_request else "memory",
            self._last_manifest.get("files", []),
        )
        return self._last_manifest

    def reload_models(self) -> Dict[str, Any]:
        manifest = self.ensure_models(force=True)
        return {
            "status": "ok",
            "message": "Cube 模型已从 Doris 重新加载并渲染",
            **manifest,
        }

    def generate_sql(self, query: CubeQuery) -> Dict[str, Any]:
        manifest = self.ensure_models()
        bundle = self._last_bundle or self._repo.load_bundle()
        sql = self._compile_sql(bundle, query)
        return {
            "status": "ok",
            "path": "cube_poc",
            "sql": sql,
            "cube_query": self._to_cube_query_payload(query),
            "model_version": manifest.get("version_no", 0),
            "model_checksum": manifest.get("checksum", ""),
            "generated_files": manifest.get("files", []),
        }

    def _compile_sql(self, bundle: CubeBundle, query: CubeQuery) -> str:
        if not query.metrics:
            raise ValueError("metrics 不能为空")

        measures = {m.measure_name: m for m in bundle.measures if m.visible}
        dimensions = {d.dimension_name: d for d in bundle.dimensions if d.visible}
        segments = {s.segment_name: s for s in bundle.segments if s.visible}
        models = {m.cube_name: m for m in bundle.models if m.visible}
        joins_by_cube: Dict[str, List[CubeJoin]] = {}
        for join in bundle.joins:
            if join.visible:
                joins_by_cube.setdefault(join.cube_name, []).append(join)

        resolved_measures = [self._resolve_measure(measures, name) for name in query.metrics]
        resolved_dimensions = [self._resolve_dimension(dimensions, name) for name in query.dimensions]
        resolved_segments = [self._resolve_segment(segments, name) for name in query.segments]

        base_cube = resolved_measures[0].cube_name
        if base_cube not in models:
            raise ValueError(f"未找到基础 Cube: {base_cube}")

        required_cubes = {base_cube}
        required_cubes.update(item.cube_name for item in resolved_dimensions)
        required_cubes.update(item.cube_name for item in resolved_segments)
        for measure in resolved_measures:
            required_cubes.update(self._referenced_cubes(measure.sql_expr, measure.cube_name))
        for dim in resolved_dimensions:
            required_cubes.update(self._referenced_cubes(dim.sql_expr, dim.cube_name))
        for segment in resolved_segments:
            required_cubes.update(self._referenced_cubes(segment.filter_sql, segment.cube_name))
        for flt in query.filters:
            dim = self._resolve_dimension(dimensions, flt.member)
            required_cubes.add(dim.cube_name)
            required_cubes.update(self._referenced_cubes(dim.sql_expr, dim.cube_name))

        join_sqls = self._resolve_joins(base_cube, required_cubes, joins_by_cube, models)
        select_clauses: List[str] = []
        group_exprs: List[str] = []

        for dim in resolved_dimensions:
            dim_expr = self._resolve_cube_sql(dim.sql_expr, dim.cube_name, base_cube)
            select_clauses.append(f"{dim_expr} AS {dim.dimension_name}")
            group_exprs.append(dim_expr)
        for measure in resolved_measures:
            select_clauses.append(
                f"{self._render_measure_expr(measure)} AS {measure.measure_name}"
            )

        where_clauses: List[str] = []
        for segment in resolved_segments:
            where_clauses.append(
                self._resolve_cube_sql(segment.filter_sql, segment.cube_name, base_cube)
            )
        for flt in query.filters:
            dim = self._resolve_dimension(dimensions, flt.member)
            where_clauses.append(self._render_filter(dim, flt))

        order_clauses: List[str] = []
        for item in query.order:
            member = item.get("member", "")
            direction = (item.get("direction") or "asc").upper()
            direction = "DESC" if direction == "DESC" else "ASC"
            if member in measures:
                order_clauses.append(f"{member} {direction}")
            elif member in dimensions:
                order_clauses.append(f"{member} {direction}")

        model = models[base_cube]
        from_clause = self._render_model_from(model, base_cube)
        if not from_clause:
            raise ValueError(f"Cube {base_cube} 未配置 sql_table / sql_expression")

        lines = [
            "SELECT",
            "  " + ",\n  ".join(select_clauses),
            f"FROM {from_clause}",
        ]
        if join_sqls:
            lines.extend(join_sqls)
        if where_clauses:
            lines.append("WHERE " + "\n  AND ".join(where_clauses))
        if group_exprs:
            lines.append("GROUP BY " + ", ".join(group_exprs))
        if order_clauses:
            lines.append("ORDER BY " + ", ".join(order_clauses))
        if query.limit:
            lines.append(f"LIMIT {int(query.limit)}")
        return "\n".join(lines)

    def _resolve_joins(
        self,
        base_cube: str,
        required_cubes: set[str],
        joins_by_cube: Dict[str, List[CubeJoin]],
        models: Dict[str, Any],
    ) -> List[str]:
        join_lines: List[str] = []
        for cube in sorted(required_cubes):
            if cube == base_cube:
                continue
            join = next(
                (
                    item
                    for item in joins_by_cube.get(base_cube, [])
                    if item.target_cube == cube and (item.join_sql or "").strip()
                ),
                None,
            )
            if join is None:
                raise ValueError(f"Cube {base_cube} 到 {cube} 缺少 join 配置")
            target = models.get(cube)
            if target is None:
                raise ValueError(f"Join 目标 Cube 不存在: {cube}")
            target_from = self._render_model_from(target, cube)
            join_type = join.join_type.upper().strip() or "LEFT"
            join_sql = self._resolve_cube_sql(join.join_sql, base_cube, base_cube)
            join_lines.append(f"{join_type} JOIN {target_from} ON {join_sql}")
        return join_lines

    def _render_measure_expr(self, measure: CubeMeasure) -> str:
        expr = self._resolve_cube_sql(measure.sql_expr or "", measure.cube_name, measure.cube_name)
        measure_type = measure.measure_type.lower().strip()
        if measure_type == "sum":
            return f"SUM({expr})"
        if measure_type == "count":
            inner = expr if expr.strip() else "*"
            return f"COUNT({inner})"
        if measure_type == "countdistinct":
            return f"COUNT(DISTINCT {expr})"
        if measure_type == "avg":
            return f"AVG({expr})"
        if measure_type == "number":
            return expr
        raise ValueError(f"暂不支持的 measure_type: {measure.measure_type}")

    def _render_filter(self, dimension: CubeDimension, flt: CubeFilter) -> str:
        expr = self._resolve_cube_sql(dimension.sql_expr or "", dimension.cube_name, dimension.cube_name)
        values = [self._map_enum_value(dimension, value) for value in flt.values]
        op = (flt.operator or "equals").lower()
        if op == "equals":
            return f"{expr} = {self._sql_literal(values[0])}"
        if op == "notequals":
            return f"{expr} <> {self._sql_literal(values[0])}"
        if op == "in":
            inner = ", ".join(self._sql_literal(item) for item in values)
            return f"{expr} IN ({inner})"
        if op == "notin":
            inner = ", ".join(self._sql_literal(item) for item in values)
            return f"{expr} NOT IN ({inner})"
        if op == "contains":
            return f"{expr} LIKE {self._sql_literal('%' + str(values[0]) + '%')}"
        if op == "gt":
            return f"{expr} > {self._sql_literal(values[0])}"
        if op == "gte":
            return f"{expr} >= {self._sql_literal(values[0])}"
        if op == "lt":
            return f"{expr} < {self._sql_literal(values[0])}"
        if op == "lte":
            return f"{expr} <= {self._sql_literal(values[0])}"
        if op == "between":
            if len(values) != 2:
                raise ValueError(f"{dimension.dimension_name} 的 between 过滤必须提供 2 个值")
            return (
                f"{expr} BETWEEN {self._sql_literal(values[0])} "
                f"AND {self._sql_literal(values[1])}"
            )
        raise ValueError(f"暂不支持的过滤操作符: {flt.operator}")

    def _to_cube_query_payload(self, query: CubeQuery) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "measures": query.metrics,
            "dimensions": query.dimensions,
            "segments": query.segments,
            "filters": [
                {"member": item.member, "operator": item.operator, "values": item.values}
                for item in query.filters
            ],
        }
        if query.order:
            payload["order"] = query.order
        if query.limit:
            payload["limit"] = query.limit
        if query.rag_hints:
            payload["ragHints"] = query.rag_hints
        return payload

    def _resolve_measure(
        self,
        measures: Dict[str, CubeMeasure],
        name: str,
    ) -> CubeMeasure:
        item = measures.get(name)
        if item is None:
            raise ValueError(f"未找到 measure: {name}")
        return item

    def _resolve_dimension(
        self,
        dimensions: Dict[str, CubeDimension],
        name: str,
    ) -> CubeDimension:
        item = dimensions.get(name)
        if item is None:
            raise ValueError(f"未找到 dimension: {name}")
        return item

    def _resolve_segment(
        self,
        segments: Dict[str, CubeSegment],
        name: str,
    ) -> CubeSegment:
        item = segments.get(name)
        if item is None:
            raise ValueError(f"未找到 segment: {name}")
        return item

    def _render_model_from(self, model: Any, alias: str) -> str:
        sql_expression = (getattr(model, "sql_expression", "") or "").strip()
        if sql_expression:
            expr = self._resolve_cube_sql(sql_expression, model.cube_name, model.cube_name)
            return f"({expr}) AS {alias}"
        table = (getattr(model, "sql_table", "") or "").strip()
        if not table:
            return ""
        return f"{table} AS {alias}"

    def _resolve_cube_sql(self, expr: str, cube_name: str, base_cube: str) -> str:
        raw = (expr or "").strip()
        if not raw:
            return raw

        def repl(match: re.Match[str]) -> str:
            token = match.group(1)
            if token == "CUBE":
                return cube_name
            return token

        resolved = _CUBE_REF_RE.sub(repl, raw)
        if _IDENTIFIER_RE.match(resolved):
            return f"{cube_name}.{resolved}"
        if "{CUBE}" in raw and "{CUBE}." not in raw:
            return resolved.replace(cube_name, cube_name)
        return resolved

    def _map_enum_value(self, dimension: CubeDimension, value: Any) -> Any:
        if value is None:
            return value
        mapping = dimension.enum_mapping or {}
        return mapping.get(str(value), value)

    def _referenced_cubes(self, expr: str, current_cube: str) -> set[str]:
        refs = set()
        for token in _CUBE_REF_RE.findall(expr or ""):
            if token == "CUBE":
                refs.add(current_cube)
            elif "." in token:
                refs.add(token.split(".", 1)[0])
        return refs

    def _sql_literal(self, value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return str(value)
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"
