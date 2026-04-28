from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import asdict, dataclass
from typing import Any, Iterable, List, Optional

from ..core.security import assert_readonly_sql
from ..doris_client import DorisClient
from .models import CubeBundle, CubeDimension, CubeMeasure
from .service import CubeQuery, CubeService

logger = logging.getLogger(__name__)


@dataclass
class CubeValidationIssue:
    run_id: str
    severity: str
    entity_type: str
    entity_name: str
    rule_code: str
    message: str
    detail: str = ""


class CubeConfigValidator:
    """Metric Cube 治理校验器。

    这里不负责修复配置，只负责把模型、指标、维度、关联、枚举值做成
    可审计的体检报告，避免坏配置直接进入问数链路。
    """

    VALID_MEASURE_TYPES = {"sum", "count", "avg", "number", "countdistinct"}
    VALID_DIMENSION_TYPES = {"string", "number", "time", "boolean"}
    VALID_JOIN_TYPES = {"LEFT", "INNER", "RIGHT", "FULL"}

    def __init__(self, client: DorisClient, service: CubeService):
        self._client = client
        self._service = service

    def validate(self, persist: bool = True, explain_sql: bool = False) -> dict[str, Any]:
        run_id = hashlib.md5(str(time.time_ns()).encode("utf-8")).hexdigest()[:12]
        started = time.time()
        manifest = self._service.ensure_models(force=True)
        bundle = self._service.get_bundle()
        issues: List[CubeValidationIssue] = []

        self._validate_models(run_id, bundle, issues, explain_sql=explain_sql)
        self._validate_measures(run_id, bundle, issues, explain_sql=explain_sql)
        self._validate_dimensions(run_id, bundle, issues, explain_sql=explain_sql)
        self._validate_joins(run_id, bundle, issues)
        self._validate_segments(run_id, bundle, issues)
        self._validate_templates(run_id, bundle, issues)
        self._validate_dimension_values(run_id, bundle, issues)

        if not issues:
            issues.append(
                CubeValidationIssue(
                    run_id=run_id,
                    severity="info",
                    entity_type="cube",
                    entity_name="all",
                    rule_code="VALIDATION_PASS",
                    message="Cube 配置校验通过",
                )
            )
        if persist:
            self._persist(run_id, issues)

        summary = self._summary(issues)
        logger.info(
            "[CubeValidator] run_id=%s errors=%s warnings=%s infos=%s elapsed_ms=%.1f",
            run_id,
            summary["error"],
            summary["warning"],
            summary["info"],
            (time.time() - started) * 1000,
        )
        return {
            "status": "ok" if summary["error"] == 0 else "error",
            "run_id": run_id,
            "summary": summary,
            "model_version": manifest.get("version_no", 0),
            "model_checksum": manifest.get("checksum", ""),
            "issues": [asdict(item) for item in issues],
            "elapsed_ms": round((time.time() - started) * 1000, 1),
        }

    def latest(self, limit: int = 200) -> dict[str, Any]:
        limit = max(1, min(int(limit or 200), 1000))
        rows = self._client.execute(
            """
            SELECT run_id
            FROM cube_store.cube_validation_results
            ORDER BY created_at DESC
            LIMIT 1
            """
        )
        if not rows:
            return {"status": "empty", "run_id": "", "summary": {}, "issues": []}
        run_id = rows[0]["run_id"]
        issues = self._client.execute(
            f"""
            SELECT run_id, severity, entity_type, entity_name, rule_code, message, detail, created_at
            FROM cube_store.cube_validation_results
            WHERE run_id = %s
            ORDER BY
              CASE severity WHEN 'error' THEN 1 WHEN 'warning' THEN 2 ELSE 3 END,
              entity_type, entity_name, rule_code
            LIMIT {limit}
            """,
            (run_id,),
        )
        summary = self._summary(
            CubeValidationIssue(
                run_id=row["run_id"],
                severity=row["severity"],
                entity_type=row["entity_type"],
                entity_name=row["entity_name"],
                rule_code=row["rule_code"],
                message=row["message"],
                detail=row.get("detail", ""),
            )
            for row in issues
        )
        return {"status": "ok", "run_id": run_id, "summary": summary, "issues": issues}

    def _validate_models(
        self,
        run_id: str,
        bundle: CubeBundle,
        issues: List[CubeValidationIssue],
        explain_sql: bool,
    ) -> None:
        seen = set()
        for model in bundle.models:
            name = model.cube_name
            if not name:
                self._issue(issues, run_id, "error", "model", "<empty>", "MODEL_NAME_EMPTY", "Cube 名称不能为空")
                continue
            if name in seen:
                self._issue(issues, run_id, "error", "model", name, "MODEL_DUPLICATE", "Cube 名称重复")
            seen.add(name)
            if not (model.sql_table or "").strip() and not (model.sql_expression or "").strip():
                self._issue(issues, run_id, "error", "model", name, "MODEL_SOURCE_EMPTY", "必须配置 sql_table 或 sql_expression")
            if (model.sql_table or "").strip() and explain_sql:
                self._explain_sql(
                    issues,
                    run_id,
                    "model",
                    name,
                    f"SELECT 1 FROM {model.sql_table} LIMIT 1",
                    "MODEL_TABLE_EXPLAIN_FAILED",
                )

    def _validate_measures(
        self,
        run_id: str,
        bundle: CubeBundle,
        issues: List[CubeValidationIssue],
        explain_sql: bool,
    ) -> None:
        model_names = {m.cube_name for m in bundle.models}
        seen = set()
        for measure in bundle.measures:
            key = (measure.cube_name, measure.measure_name)
            label = f"{measure.cube_name}.{measure.measure_name}"
            if key in seen:
                self._issue(issues, run_id, "error", "measure", label, "MEASURE_DUPLICATE", "指标重复")
            seen.add(key)
            if measure.cube_name not in model_names:
                self._issue(issues, run_id, "error", "measure", label, "MEASURE_MODEL_MISSING", "指标归属 Cube 不存在")
            if not (measure.sql_expr or "").strip():
                self._issue(issues, run_id, "error", "measure", label, "MEASURE_EXPR_EMPTY", "指标 sql_expr 不能为空")
            if (measure.measure_type or "").lower() not in self.VALID_MEASURE_TYPES:
                self._issue(issues, run_id, "error", "measure", label, "MEASURE_TYPE_INVALID", f"不支持的指标类型: {measure.measure_type}")
            if explain_sql:
                self._compile_and_explain_measure(run_id, issues, measure)

    def _validate_dimensions(
        self,
        run_id: str,
        bundle: CubeBundle,
        issues: List[CubeValidationIssue],
        explain_sql: bool,
    ) -> None:
        model_names = {m.cube_name for m in bundle.models}
        seen = set()
        for dim in bundle.dimensions:
            key = (dim.cube_name, dim.dimension_name)
            label = f"{dim.cube_name}.{dim.dimension_name}"
            if key in seen:
                self._issue(issues, run_id, "error", "dimension", label, "DIMENSION_DUPLICATE", "维度重复")
            seen.add(key)
            if dim.cube_name not in model_names:
                self._issue(issues, run_id, "error", "dimension", label, "DIMENSION_MODEL_MISSING", "维度归属 Cube 不存在")
            if not (dim.sql_expr or "").strip():
                self._issue(issues, run_id, "error", "dimension", label, "DIMENSION_EXPR_EMPTY", "维度 sql_expr 不能为空")
            if (dim.dimension_type or "").lower() not in self.VALID_DIMENSION_TYPES:
                self._issue(issues, run_id, "error", "dimension", label, "DIMENSION_TYPE_INVALID", f"不支持的维度类型: {dim.dimension_type}")
            if dim.enum_mapping and not isinstance(dim.enum_mapping, dict):
                self._issue(issues, run_id, "error", "dimension", label, "DIMENSION_ENUM_INVALID", "enum_mapping_json 必须是 JSON Object")
            if explain_sql:
                self._compile_and_explain_dimension(run_id, issues, bundle, dim)

    def _validate_joins(self, run_id: str, bundle: CubeBundle, issues: List[CubeValidationIssue]) -> None:
        model_names = {m.cube_name for m in bundle.models}
        for join in bundle.joins:
            label = f"{join.cube_name}->{join.target_cube}"
            if join.cube_name not in model_names:
                self._issue(issues, run_id, "error", "join", label, "JOIN_LEFT_MODEL_MISSING", "左侧 Cube 不存在")
            if join.target_cube not in model_names:
                self._issue(issues, run_id, "error", "join", label, "JOIN_RIGHT_MODEL_MISSING", "右侧 Cube 不存在")
            if not (join.join_sql or "").strip():
                self._issue(issues, run_id, "error", "join", label, "JOIN_SQL_EMPTY", "join_sql 不能为空")
            if (join.join_type or "").upper() not in self.VALID_JOIN_TYPES:
                self._issue(issues, run_id, "warning", "join", label, "JOIN_TYPE_UNUSUAL", f"非常规 join_type: {join.join_type}")

    def _validate_segments(self, run_id: str, bundle: CubeBundle, issues: List[CubeValidationIssue]) -> None:
        model_names = {m.cube_name for m in bundle.models}
        for segment in bundle.segments:
            label = f"{segment.cube_name}.{segment.segment_name}"
            if segment.cube_name not in model_names:
                self._issue(issues, run_id, "error", "segment", label, "SEGMENT_MODEL_MISSING", "分段归属 Cube 不存在")
            if not (segment.filter_sql or "").strip():
                self._issue(issues, run_id, "error", "segment", label, "SEGMENT_FILTER_EMPTY", "filter_sql 不能为空")

    def _validate_templates(self, run_id: str, bundle: CubeBundle, issues: List[CubeValidationIssue]) -> None:
        for template in bundle.templates:
            if not (template.template_sql or "").strip():
                self._issue(issues, run_id, "warning", "template", template.template_name, "TEMPLATE_SQL_EMPTY", "模板 SQL 为空")
            if template.params and not isinstance(template.params, dict):
                self._issue(issues, run_id, "error", "template", template.template_name, "TEMPLATE_PARAMS_INVALID", "params_json 必须是 JSON Object")

    def _validate_dimension_values(self, run_id: str, bundle: CubeBundle, issues: List[CubeValidationIssue]) -> None:
        dim_keys = {(d.cube_name, d.dimension_name) for d in bundle.dimensions}
        seen = set()
        for value in bundle.dimension_values:
            label = f"{value.cube_name}.{value.dimension_name}:{value.value_code}"
            key = (value.cube_name, value.dimension_name, value.value_code)
            if (value.cube_name, value.dimension_name) not in dim_keys:
                self._issue(issues, run_id, "warning", "dimension_value", label, "ENUM_DIMENSION_MISSING", "枚举值对应维度不存在")
            if key in seen:
                self._issue(issues, run_id, "warning", "dimension_value", label, "ENUM_DUPLICATE", "枚举值重复")
            seen.add(key)

    def _compile_and_explain_measure(
        self,
        run_id: str,
        issues: List[CubeValidationIssue],
        measure: CubeMeasure,
    ) -> None:
        label = f"{measure.cube_name}.{measure.measure_name}"
        try:
            result = self._service.generate_sql(CubeQuery(metrics=[measure.measure_name], limit=1))
            sql = result["sql"]
            assert_readonly_sql(sql)
            self._explain_sql(issues, run_id, "measure", label, sql, "MEASURE_SQL_EXPLAIN_FAILED")
        except Exception as exc:
            self._issue(issues, run_id, "error", "measure", label, "MEASURE_COMPILE_FAILED", "指标 SQL 编译失败", str(exc))

    def _compile_and_explain_dimension(
        self,
        run_id: str,
        issues: List[CubeValidationIssue],
        bundle: CubeBundle,
        dim: CubeDimension,
    ) -> None:
        label = f"{dim.cube_name}.{dim.dimension_name}"
        measure = next((m for m in bundle.measures if m.cube_name == dim.cube_name), None)
        if measure is None:
            self._issue(issues, run_id, "warning", "dimension", label, "DIMENSION_NO_SMOKE_MEASURE", "同 Cube 下没有指标，跳过 SQL 试编译")
            return
        try:
            result = self._service.generate_sql(
                CubeQuery(metrics=[measure.measure_name], dimensions=[dim.dimension_name], limit=1)
            )
            sql = result["sql"]
            assert_readonly_sql(sql)
            self._explain_sql(issues, run_id, "dimension", label, sql, "DIMENSION_SQL_EXPLAIN_FAILED")
        except Exception as exc:
            self._issue(issues, run_id, "error", "dimension", label, "DIMENSION_COMPILE_FAILED", "维度 SQL 编译失败", str(exc))

    def _explain_sql(
        self,
        issues: List[CubeValidationIssue],
        run_id: str,
        entity_type: str,
        entity_name: str,
        sql: str,
        rule_code: str,
    ) -> None:
        try:
            assert_readonly_sql(sql)
            self._client.execute("EXPLAIN " + sql)
        except Exception as exc:
            self._issue(issues, run_id, "error", entity_type, entity_name, rule_code, "Doris EXPLAIN 校验失败", str(exc))

    def _persist(self, run_id: str, issues: List[CubeValidationIssue]) -> None:
        for idx, issue in enumerate(issues, start=1):
            validation_id = int(hashlib.md5(f"{run_id}:{idx}:{issue.rule_code}".encode("utf-8")).hexdigest()[:15], 16)
            self._client.execute_write(
                """
                INSERT INTO cube_store.cube_validation_results
                  (validation_id, run_id, severity, entity_type, entity_name, rule_code, message, detail)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    validation_id,
                    issue.run_id,
                    issue.severity,
                    issue.entity_type,
                    issue.entity_name,
                    issue.rule_code,
                    issue.message,
                    issue.detail[:4000],
                ),
            )

    def _issue(
        self,
        issues: List[CubeValidationIssue],
        run_id: str,
        severity: str,
        entity_type: str,
        entity_name: str,
        rule_code: str,
        message: str,
        detail: str = "",
    ) -> None:
        issues.append(
            CubeValidationIssue(
                run_id=run_id,
                severity=severity,
                entity_type=entity_type,
                entity_name=entity_name,
                rule_code=rule_code,
                message=message,
                detail=detail,
            )
        )

    def _summary(self, issues: Iterable[CubeValidationIssue]) -> dict[str, int]:
        summary = {"error": 0, "warning": 0, "info": 0}
        for issue in issues:
            severity = issue.severity if issue.severity in summary else "info"
            summary[severity] += 1
        return summary
