from __future__ import annotations

import hashlib
import json
import logging
from typing import Any, Dict, List

from ..doris_client import DorisClient
from .models import (
    CubeBundle,
    CubeDimension,
    CubeDimensionValue,
    CubeJoin,
    CubeMeasure,
    CubeModel,
    CubeSemanticAlias,
    CubeSegment,
    CubeTemplate,
)

logger = logging.getLogger(__name__)


class CubeStoreRepository:
    def __init__(self, client: DorisClient):
        self._db = client

    def get_latest_version(self) -> Dict[str, Any]:
        rows = self._db.execute(
            """
            SELECT version_no, checksum, status, updated_at
            FROM cube_store.cube_model_versions
            WHERE status = 'active'
            ORDER BY version_no DESC, updated_at DESC
            LIMIT 1
            """
        )
        if rows:
            return rows[0]
        return {"version_no": 0, "checksum": "", "status": "missing"}

    def load_bundle(self) -> CubeBundle:
        models = [
            CubeModel(
                cube_name=row["cube_name"],
                title=row.get("title", ""),
                sql_table=row.get("sql_table", ""),
                sql_expression=row.get("sql_expression", ""),
                data_source=row.get("data_source") or "default",
                public_flag=bool(row.get("public_flag", 1)),
                visible=bool(row.get("visible", 1)),
            )
            for row in self._db.execute(
                """
                SELECT cube_name, title, sql_table, sql_expression, data_source, public_flag, visible
                FROM cube_store.cube_models
                WHERE visible = 1
                ORDER BY cube_name
                """
            )
        ]
        measures = [
            CubeMeasure(
                cube_name=row["cube_name"],
                measure_name=row["measure_name"],
                title=row["title"],
                description=row.get("description", ""),
                sql_expr=row["sql_expr"],
                measure_type=row["measure_type"],
                format=row.get("format") or "number",
                drill_members=_json_list(row.get("drill_members_json")),
                visible=bool(row.get("visible", 1)),
            )
            for row in self._db.execute(
                """
                SELECT cube_name, measure_name, title, description, sql_expr, measure_type,
                       format, drill_members_json, visible
                FROM cube_store.cube_measures
                WHERE visible = 1
                ORDER BY cube_name, measure_name
                """
            )
        ]
        dimensions = [
            CubeDimension(
                cube_name=row["cube_name"],
                dimension_name=row["dimension_name"],
                title=row["title"],
                description=row.get("description", ""),
                sql_expr=row["sql_expr"],
                dimension_type=row["dimension_type"],
                primary_key_flag=bool(row.get("primary_key_flag", 0)),
                enum_mapping=_json_dict(row.get("enum_mapping_json")),
                hierarchy=_json_list(row.get("hierarchy_json")),
                visible=bool(row.get("visible", 1)),
            )
            for row in self._db.execute(
                """
                SELECT cube_name, dimension_name, title, description, sql_expr,
                       dimension_type, primary_key_flag, enum_mapping_json,
                       hierarchy_json, visible
                FROM cube_store.cube_dimensions
                WHERE visible = 1
                ORDER BY cube_name, dimension_name
                """
            )
        ]
        dimension_values = self._load_dimension_values()
        _merge_dimension_values(dimensions, dimension_values)
        joins = [
            CubeJoin(
                cube_name=row["cube_name"],
                target_cube=row["target_cube"],
                relationship=row["relationship"],
                join_type=row.get("join_type") or "LEFT",
                join_sql=row["join_sql"],
                visible=bool(row.get("visible", 1)),
            )
            for row in self._db.execute(
                """
                SELECT cube_name, target_cube, relationship, join_type, join_sql, visible
                FROM cube_store.cube_joins
                WHERE visible = 1
                ORDER BY cube_name, target_cube
                """
            )
        ]
        segments = [
            CubeSegment(
                cube_name=row["cube_name"],
                segment_name=row["segment_name"],
                title=row["title"],
                description=row.get("description", ""),
                filter_sql=row["filter_sql"],
                visible=bool(row.get("visible", 1)),
            )
            for row in self._db.execute(
                """
                SELECT cube_name, segment_name, title, description, filter_sql, visible
                FROM cube_store.cube_segments
                WHERE visible = 1
                ORDER BY cube_name, segment_name
                """
            )
        ]
        templates = [
            CubeTemplate(
                template_name=row["template_name"],
                template_type=row["template_type"],
                title=row["title"],
                template_sql=row["template_sql"],
                params=_json_dict(row.get("params_json")),
                visible=bool(row.get("visible", 1)),
            )
            for row in self._db.execute(
                """
                SELECT template_name, template_type, title, template_sql, params_json, visible
                FROM cube_store.cube_sql_templates
                WHERE visible = 1
                ORDER BY template_type, template_name
                """
            )
        ]
        aliases = self._load_semantic_aliases()
        payload = {
            "models": [m.__dict__ for m in models],
            "measures": [m.__dict__ for m in measures],
            "dimensions": [d.__dict__ for d in dimensions],
            "dimension_values": [v.__dict__ for v in dimension_values],
            "joins": [j.__dict__ for j in joins],
            "segments": [s.__dict__ for s in segments],
            "templates": [t.__dict__ for t in templates],
            "aliases": [a.__dict__ for a in aliases],
        }
        checksum = hashlib.md5(
            json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        version = self.get_latest_version()
        return CubeBundle(
            models=models,
            measures=measures,
            dimensions=dimensions,
            dimension_values=dimension_values,
            joins=joins,
            segments=segments,
            templates=templates,
            aliases=aliases,
            version_no=int(version.get("version_no", 0) or 0),
            checksum=checksum,
        )

    def _load_semantic_aliases(self) -> List[CubeSemanticAlias]:
        try:
            rows = self._db.execute(
                """
                SELECT entity_type, entity_name, alias_text, source, weight, match_type, visible
                FROM cube_store.cube_semantic_aliases
                WHERE visible = 1
                ORDER BY weight DESC, LENGTH(alias_text) DESC, alias_text
                """
            )
        except Exception as exc:
            if "cube_semantic_aliases" in str(exc) and "does not exist" in str(exc).lower():
                logger.warning(
                    "[CubeStore] cube_semantic_aliases 不存在，跳过语义别名增强；"
                    "执行 sql/cube_store.sql 后可启用通用别名维护。"
                )
                return []
            raise
        return [
            CubeSemanticAlias(
                entity_type=str(row.get("entity_type") or ""),
                entity_name=str(row.get("entity_name") or ""),
                alias_text=str(row.get("alias_text") or ""),
                source=str(row.get("source") or "manual"),
                weight=float(row.get("weight", 1.0) or 1.0),
                match_type=str(row.get("match_type") or "contains"),
                visible=bool(row.get("visible", 1)),
            )
            for row in rows
        ]

    def _load_dimension_values(self) -> List[CubeDimensionValue]:
        try:
            rows = self._db.execute(
                """
                SELECT cube_name, dimension_name, value_code, value_label,
                       aliases_json, source, usage_count, visible
                FROM cube_store.cube_dimension_values
                WHERE visible = 1
                ORDER BY cube_name, dimension_name, usage_count DESC, value_label
                """
            )
        except Exception as exc:
            if "cube_dimension_values" in str(exc) and "does not exist" in str(exc).lower():
                logger.warning(
                    "[CubeStore] cube_dimension_values 不存在，跳过枚举增强；"
                    "执行 sql/cube_store.sql 后可启用枚举表维护。"
                )
                return []
            raise
        return [
            CubeDimensionValue(
                cube_name=row["cube_name"],
                dimension_name=row["dimension_name"],
                value_code=str(row.get("value_code") or ""),
                value_label=str(row.get("value_label") or row.get("value_code") or ""),
                aliases=_json_list(row.get("aliases_json")),
                source=row.get("source") or "manual",
                usage_count=int(row.get("usage_count", 0) or 0),
                visible=bool(row.get("visible", 1)),
            )
            for row in rows
        ]


def _json_list(value: Any) -> List[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    try:
        parsed = json.loads(value)
        if isinstance(parsed, list):
            return [str(item) for item in parsed]
    except Exception:
        pass
    return []


def _json_dict(value: Any) -> Dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass
    return {}


def _merge_dimension_values(
    dimensions: List[CubeDimension],
    values: List[CubeDimensionValue],
) -> None:
    by_key = {(d.cube_name, d.dimension_name): d for d in dimensions}
    for item in values:
        dim = by_key.get((item.cube_name, item.dimension_name))
        if dim is None:
            continue
        target = item.value_code
        if not target:
            continue
        candidates = [item.value_code, item.value_label, *item.aliases]
        for candidate in candidates:
            key = str(candidate or "").strip()
            if not key:
                continue
            # enum_mapping_json 是人工强配置，优先级高于自动采集值。
            dim.enum_mapping.setdefault(key, target)
