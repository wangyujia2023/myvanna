"""
Vanna Skill REST API  +  SSE 实时推送
运行: uvicorn api.main:app --host 0.0.0.0 --port 8765 --reload
"""
import json
import logging
import hashlib
import queue as queue_module
import re
import sys
import threading
from pathlib import Path
from typing import Any, Optional

import pandas as pd
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from vanna_skill import DorisVanna, tracer, load_config, save_config, AskLCPipeline, DorisClient, PromptStore, LineageManager, invalidate_lineage_cache, CubeService, CubePipeline
from vanna_skill.pipelines.semantic_pipeline import SemanticPipeline
from vanna_skill.semantic.semantic_sql_rag import SemanticSQLRAGStore
from vanna_skill.semantic.catalog import invalidate_semantic_cache
from vanna_skill.semantic.schema_scanner import SchemaScanner
from vanna_skill.cube.service import CubeFilter, CubeQuery
from vanna_skill.cube.validator import CubeConfigValidator
from vanna_skill.core.security import assert_readonly_sql
from vanna_skill.rca import RCAService
from vanna_skill.rca.smart_pipeline import SmartRCAPipeline

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s  %(message)s")
logger = logging.getLogger(__name__)

# ── 配置（统一从 config.json 读取）───────────────────────────────────────────
CONFIG = load_config()

# ── FastAPI ──────────────────────────────────────────────────────────────────
app = FastAPI(title="Vanna Skill API", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

_vanna: Optional[DorisVanna] = None
_ask_lc_pipeline: Optional[AskLCPipeline] = None
_semantic_pipeline: Optional[SemanticPipeline] = None
_prompt_store: Optional[PromptStore] = None
_cube_service: Optional[CubeService] = None
_cube_pipeline: Optional[CubePipeline] = None
_cube_admin_client: Optional[DorisClient] = None
_cube_validator: Optional[CubeConfigValidator] = None
_rca_service: Optional[RCAService] = None
_smart_rca_pipeline: Optional[SmartRCAPipeline] = None
_rca_admin_client: Optional[DorisClient] = None
_execute_client: Optional[DorisClient] = None


CUBE_ADMIN_ENTITIES: dict[str, dict[str, Any]] = {
    "models": {
        "table": "cube_models",
        "pk": "model_id",
        "order": "cube_name",
        "fields": ["model_id", "cube_name", "title", "sql_table", "sql_expression", "data_source", "public_flag", "visible", "version"],
    },
    "measures": {
        "table": "cube_measures",
        "pk": "measure_id",
        "order": "cube_name, measure_name",
        "fields": ["measure_id", "cube_name", "measure_name", "title", "description", "sql_expr", "measure_type", "format", "drill_members_json", "visible", "version"],
    },
    "dimensions": {
        "table": "cube_dimensions",
        "pk": "dimension_id",
        "order": "cube_name, dimension_name",
        "fields": ["dimension_id", "cube_name", "dimension_name", "title", "description", "sql_expr", "dimension_type", "primary_key_flag", "enum_mapping_json", "hierarchy_json", "visible", "version"],
    },
    "dimension-values": {
        "table": "cube_dimension_values",
        "pk": "value_id",
        "order": "cube_name, dimension_name, usage_count DESC, value_label",
        "fields": ["value_id", "cube_name", "dimension_name", "value_code", "value_label", "aliases_json", "source", "source_table", "source_column", "usage_count", "visible", "version"],
    },
    "joins": {
        "table": "cube_joins",
        "pk": "join_id",
        "order": "cube_name, target_cube",
        "fields": ["join_id", "cube_name", "target_cube", "relationship", "join_type", "join_sql", "visible", "version"],
    },
    "segments": {
        "table": "cube_segments",
        "pk": "segment_id",
        "order": "cube_name, segment_name",
        "fields": ["segment_id", "cube_name", "segment_name", "title", "description", "filter_sql", "visible", "version"],
    },
    "templates": {
        "table": "cube_sql_templates",
        "pk": "template_id",
        "order": "template_type, template_name",
        "fields": ["template_id", "template_name", "template_type", "title", "template_sql", "params_json", "visible", "version"],
    },
    "semantic-aliases": {
        "table": "cube_semantic_aliases",
        "pk": "alias_id",
        "order": "entity_type, entity_name, weight DESC, alias_text",
        "fields": ["alias_id", "entity_type", "entity_name", "alias_text", "source", "match_type", "weight", "visible", "version"],
    },
    "versions": {
        "table": "cube_model_versions",
        "pk": "version_id",
        "order": "version_no DESC, updated_at DESC",
        "fields": ["version_id", "version_no", "checksum", "status", "remark"],
    },
    "validation-results": {
        "table": "cube_validation_results",
        "pk": "validation_id",
        "order": "created_at DESC",
        "fields": ["validation_id", "run_id", "severity", "entity_type", "entity_name", "rule_code", "message", "detail"],
        "timestamp": "created_at",
    },
    "regression-cases": {
        "table": "cube_regression_cases",
        "pk": "case_id",
        "order": "updated_at DESC",
        "fields": ["case_id", "question", "expected_sql", "expected_metrics_json", "expected_dimensions_json", "expected_filters_json", "tags_json", "status", "last_run_status", "last_run_detail"],
    },
    "publish-history": {
        "table": "cube_publish_history",
        "pk": "publish_id",
        "order": "created_at DESC",
        "fields": ["publish_id", "version_no", "checksum", "operator", "status", "validation_run_id", "remark"],
        "timestamp": "created_at",
    },
    "metric-influences": {
        "table": "cube_metric_influences",
        "pk": "influence_id",
        "order": "target_metric, source_metric",
        "fields": ["influence_id", "source_metric", "target_metric", "relation_type", "weight", "direction", "description", "visible"],
    },
}


RCA_ADMIN_ENTITIES: dict[str, dict[str, Any]] = {
    "nodes": {
        "table": "rca_nodes",
        "pk": "node_id",
        "order": "node_type, node_name",
        "fields": ["node_id", "node_name", "node_type", "title", "description", "cube_ref", "expression", "enabled", "version"],
    },
    "edges": {
        "table": "rca_edges",
        "pk": "edge_id",
        "order": "target_node, ABS(prior_strength * confidence) DESC, source_node",
        "fields": ["edge_id", "source_node", "target_node", "edge_type", "direction", "prior_strength", "confidence", "lag", "condition_json", "evidence_json", "enabled", "version"],
    },
    "causal-specs": {
        "table": "rca_causal_specs",
        "pk": "spec_id",
        "order": "outcome_node, treatment_node",
        "fields": ["spec_id", "treatment_node", "outcome_node", "common_causes_json", "instruments_json", "effect_modifiers_json", "graph_gml", "estimator", "refuters_json", "enabled", "version"],
    },
    "profiles": {
        "table": "rca_profiles",
        "pk": "profile_id",
        "order": "metric_name, profile_name",
        "fields": ["profile_id", "profile_name", "metric_name", "default_dimensions_json", "default_baseline", "min_contribution", "max_depth", "algorithm", "enabled", "description"],
    },
    "runs": {
        "table": "rca_runs",
        "pk": "run_id",
        "order": "created_at DESC",
        "fields": ["run_id", "question", "metric_name", "status", "plan_json", "candidates_json", "causal_results_json", "report_text"],
        "timestamp": "created_at",
        "readonly": True,
    },
    "run-candidates": {
        "table": "rca_run_candidates",
        "pk": "candidate_id",
        "order": "run_id, rank_no",
        "fields": ["candidate_id", "run_id", "rank_no", "candidate_type", "candidate_json", "runtime_contribution", "prior_score", "causal_score", "final_score"],
        "timestamp": "created_at",
        "readonly": True,
    },
}


def get_prompt_store() -> PromptStore:
    global _prompt_store, CONFIG
    if _prompt_store is None:
        _prompt_store = PromptStore(
            DorisClient(
                host=CONFIG["host"],
                port=CONFIG["port"],
                user=CONFIG["user"],
                password=CONFIG.get("password", ""),
                database="vanna_store",
            )
        )
    return _prompt_store


def build_runtime_config() -> dict:
    runtime = CONFIG.copy()
    try:
        runtime.update(
            get_prompt_store().get_prompt_state(runtime.get("initial_prompt", ""))
        )
    except Exception as e:
        logger.warning(f"[PromptStore] 加载 Prompt 配置失败，回退文件配置: {e}")
    return runtime

def get_vanna() -> DorisVanna:
    global _vanna, CONFIG
    if _vanna is None:
        _vanna = DorisVanna(build_runtime_config())
    return _vanna


def get_ask_lc_pipeline() -> AskLCPipeline:
    global _ask_lc_pipeline, CONFIG
    if _ask_lc_pipeline is None:
        _ask_lc_pipeline = AskLCPipeline(build_runtime_config())
    return _ask_lc_pipeline


def get_semantic_pipeline() -> SemanticPipeline:
    global _semantic_pipeline, CONFIG
    if _semantic_pipeline is None:
        _semantic_pipeline = SemanticPipeline(build_runtime_config())
    return _semantic_pipeline


def get_cube_service() -> CubeService:
    global _cube_service, CONFIG
    if _cube_service is None:
        _cube_service = CubeService(CONFIG.copy())
    return _cube_service


def get_cube_pipeline() -> CubePipeline:
    global _cube_pipeline, CONFIG
    if _cube_pipeline is None:
        _cube_pipeline = CubePipeline(CONFIG.copy(), get_cube_service())
    return _cube_pipeline


def get_cube_admin_client() -> DorisClient:
    global _cube_admin_client, CONFIG
    if _cube_admin_client is None:
        _cube_admin_client = DorisClient(
            host=CONFIG["host"],
            port=CONFIG["port"],
            user=CONFIG["user"],
            password=CONFIG.get("password", ""),
            database=CONFIG.get("cube_store_database", "cube_store"),
        )
    return _cube_admin_client


def get_rca_admin_client() -> DorisClient:
    global _rca_admin_client, CONFIG
    if _rca_admin_client is None:
        _rca_admin_client = DorisClient(
            host=CONFIG["host"],
            port=CONFIG["port"],
            user=CONFIG["user"],
            password=CONFIG.get("password", ""),
            database=CONFIG.get("rca_store_database", "rca_store"),
        )
    return _rca_admin_client


def get_cube_validator() -> CubeConfigValidator:
    global _cube_validator
    if _cube_validator is None:
        _cube_validator = CubeConfigValidator(get_cube_admin_client(), get_cube_service())
    return _cube_validator


def get_rca_service() -> RCAService:
    global _rca_service
    if _rca_service is None:
        _rca_service = RCAService(CONFIG.copy(), get_cube_service())
    return _rca_service


def get_smart_rca_pipeline() -> SmartRCAPipeline:
    global _smart_rca_pipeline
    if _smart_rca_pipeline is None:
        _smart_rca_pipeline = SmartRCAPipeline(CONFIG.copy(), get_cube_service(), get_rca_service())
    return _smart_rca_pipeline


def get_execute_client() -> DorisClient:
    """Lightweight business DB client for manual SQL execution."""
    global _execute_client, CONFIG
    if _execute_client is None:
        _execute_client = DorisClient(
            host=CONFIG["host"],
            port=CONFIG["port"],
            user=CONFIG["user"],
            password=CONFIG.get("password", ""),
            database=CONFIG.get("database", "retail_dw"),
        )
    return _execute_client


def _cube_entity_meta(entity: str) -> dict[str, Any]:
    meta = CUBE_ADMIN_ENTITIES.get(entity)
    if not meta:
        raise HTTPException(404, f"未知 Cube 实体: {entity}")
    return meta


def _rca_entity_meta(entity: str) -> dict[str, Any]:
    meta = RCA_ADMIN_ENTITIES.get(entity)
    if not meta:
        raise HTTPException(404, f"未知 RCA 实体: {entity}")
    return meta


def _stable_bigint(*parts: Any) -> int:
    raw = "\n".join(str(part or "") for part in parts)
    return int(hashlib.md5(raw.encode("utf-8")).hexdigest()[:15], 16)


def _safe_db_name(value: str, default: str) -> str:
    name = str(value or default).strip() or default
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise HTTPException(400, f"非法数据库名: {name}")
    return name


def _cube_store_db() -> str:
    return _safe_db_name(CONFIG.get("cube_store_database", "cube_store"), "cube_store")


def _rca_store_db() -> str:
    return _safe_db_name(CONFIG.get("rca_store_database", "rca_store"), "rca_store")


def _cube_primary_key_columns(rows: list[dict[str, Any]], simple_col: re.Pattern[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for row in rows:
        cube_name = str(row.get("cube_name") or "")
        column = str(row.get("sql_expr") or "").strip()
        if cube_name and row.get("primary_key_flag") and simple_col.match(column):
            result.setdefault(cube_name, column)
    return result


def _cube_label_columns(rows: list[dict[str, Any]], simple_col: re.Pattern[str]) -> dict[str, str]:
    result: dict[str, str] = {}
    for row in rows:
        cube_name = str(row.get("cube_name") or "")
        dimension_name = str(row.get("dimension_name") or "")
        column = str(row.get("sql_expr") or "").strip()
        if not cube_name or not simple_col.match(column):
            continue
        if dimension_name.endswith("_name") or dimension_name in {"name", "title"}:
            result.setdefault(cube_name, column)
    return result


def _enum_business_group(cube_name: str, dimension_name: str) -> tuple[str, str, int]:
    if cube_name == "cities":
        return "city", "城市", 10
    if cube_name == "member_types":
        return "member", "会员", 20
    if cube_name == "stores":
        return "store", "门店", 30
    if cube_name in {"orders", "users", "refunds"}:
        return "detail", "明细表", 90
    return "other", "其他维度", 80


def _enum_recommended(cube_name: str, dimension_name: str, eligible: bool) -> bool:
    if not eligible:
        return False
    recommended = {
        "cities": {"city_name", "region_name"},
        "member_types": {"member_type", "member_level"},
        "stores": {"store_name", "store_type"},
    }
    return dimension_name in recommended.get(cube_name, set())


def _enum_option_visible(cube_name: str, dimension_name: str) -> bool:
    # 门店 ID 是门店名称枚举的稳定 value_code，不作为独立业务枚举展示。
    return not (cube_name == "stores" and dimension_name == "store_id")


def _enum_option_title(cube_name: str, dimension_name: str, title: str) -> str:
    if cube_name == "stores" and dimension_name == "store_name":
        return "门店"
    return title or dimension_name


def _enum_collect_columns(
    cube_name: str,
    dimension_name: str,
    dimension_column: str,
    key_columns: dict[str, str],
    label_columns: dict[str, str],
) -> tuple[str, str]:
    """Return (code_column, label_column) for enum collection.

    Display dimensions such as cities.city_name should store the stable business
    key as value_code and the human-readable text as value_label.
    """
    code_column = dimension_column
    label_column = dimension_column
    key_column = key_columns.get(cube_name)
    label_candidate = label_columns.get(cube_name)
    if key_column and dimension_name in {"city_name", "member_type", "store_name"}:
        code_column = key_column
        label_column = dimension_column
    elif (
        key_column
        and (dimension_name.endswith("_code") or dimension_name.endswith("_id"))
        and label_candidate
        and label_candidate != dimension_column
    ):
        code_column = dimension_column
        label_column = label_candidate
    return code_column, label_column


def validate_readonly_sql(sql: str) -> None:
    try:
        assert_readonly_sql(sql)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc

# ── 前端静态文件 ──────────────────────────────────────────────────────────────
UI_DIR = Path(__file__).parent.parent / "ui"
app.mount("/ui", StaticFiles(directory=str(UI_DIR)), name="ui")

@app.get("/", response_class=FileResponse)
async def index():
    return FileResponse(str(UI_DIR / "index.html"))


# ════════════════════════════════════════════════════════════════════════════
# SSE 实时流式问答（核心接口）
# ════════════════════════════════════════════════════════════════════════════

@app.get("/ask/stream")
def ask_stream(q: str = Query(..., description="自然语言问题")):
    """
    SSE 实时推送每个步骤状态，供前端渲染调用链路。
    事件类型: start | step_start | step_done | final | error
    """
    step_queue: queue_module.Queue = queue_module.Queue()

    def callback(event_type: str, data: dict):
        step_queue.put((event_type, data))

    def run_ask():
        try:
            get_vanna().ask_with_trace(q, step_callback=callback)
        except Exception as e:
            step_queue.put(("error", {"error": str(e)}))

    thread = threading.Thread(target=run_ask, daemon=True)
    thread.start()

    def event_generator():
        while True:
            try:
                event_type, data = step_queue.get(timeout=60)
                payload = json.dumps(data, ensure_ascii=False)
                yield f"event: {event_type}\ndata: {payload}\n\n"
                if event_type in ("final", "error"):
                    break
            except queue_module.Empty:
                yield ": keepalive\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.get("/ask-lc/stream")
def ask_lc_stream(
    q: str = Query(..., description="自然语言问题"),
    prompt_version: Optional[str] = Query(None, description="Prompt 版本"),
):
    step_queue: queue_module.Queue = queue_module.Queue()

    def callback(event_type: str, data: dict):
        step_queue.put((event_type, data))

    def run_ask():
        try:
            get_ask_lc_pipeline().run_with_trace(
                q,
                prompt_version=prompt_version,
                step_callback=callback,
            )
        except Exception as e:
            step_queue.put(("error", {"error": str(e)}))

    thread = threading.Thread(target=run_ask, daemon=True)
    thread.start()

    def event_generator():
        while True:
            try:
                event_type, data = step_queue.get(timeout=60)
                payload = json.dumps(data, ensure_ascii=False)
                yield f"event: {event_type}\ndata: {payload}\n\n"
                if event_type in ("final", "error"):
                    break
            except queue_module.Empty:
                yield ": keepalive\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


# ════════════════════════════════════════════════════════════════════════════
# 普通 JSON 接口
# ════════════════════════════════════════════════════════════════════════════

class AskRequest(BaseModel):
    question: str = Field(..., min_length=2)
    prompt_version: Optional[str] = None

class TrainSqlRequest(BaseModel):
    question: str
    sql: str
    source: str = "api"

class TrainDdlRequest(BaseModel):
    ddl: str
    source: str = "api"

class TrainDocRequest(BaseModel):
    documentation: str
    source: str = "api"

class FeedbackRequest(BaseModel):
    question: str
    sql: str
    is_correct: bool
    corrected_sql: Optional[str] = None
    engine: str = "vanna"

class ConfigRequest(BaseModel):
    host: str
    port: int
    user: str
    password: str
    database: str
    qwen_api_key: str
    model: str = "qwen-plus"
    n_results: int = 5
    langchain_fallback_enabled: bool = False
    semantic_to_langchain_fallback_enabled: bool = False
    semantic_sql_rag_enabled: bool = False
    cube_store_database: str = "cube_store"
    rca_store_database: str = "rca_store"
    cube_model_reload_each_request: bool = False
    cube_default_time_scope: str = ""


class SystemPromptRequest(BaseModel):
    initial_prompt: str = ""


class PromptVersionRequest(BaseModel):
    id: str
    name: str
    description: str = ""
    system_prompt: str = ""


class PromptActivateRequest(BaseModel):
    version_id: str


class ABTestRequest(BaseModel):
    enabled: bool = False
    version_a: str
    version_b: str = ""


class CubeFilterRequest(BaseModel):
    member: str
    operator: str = "equals"
    values: list[Any] = Field(default_factory=list)


class CubeGenerateSQLRequest(BaseModel):
    metrics: list[str] = Field(default_factory=list)
    dimensions: list[str] = Field(default_factory=list)
    filters: list[CubeFilterRequest] = Field(default_factory=list)
    segments: list[str] = Field(default_factory=list)
    order: list[dict[str, str]] = Field(default_factory=list)
    limit: Optional[int] = None
    rag_hints: list[dict[str, Any]] = Field(default_factory=list)


class CubeAdminUpsertRequest(BaseModel):
    row: dict[str, Any]


class CubeEnumCollectRequest(BaseModel):
    max_values: int = 200
    max_cardinality: int = 500
    include_cubes: list[str] = Field(default_factory=list)
    include_dimensions: list[str] = Field(default_factory=list)
    exclude_cubes: list[str] = Field(default_factory=lambda: ["orders", "users", "refunds"])


class CubeValidateRequest(BaseModel):
    explain_sql: bool = False
    persist: bool = True


class RCAFilterRequest(BaseModel):
    member: str
    operator: str = "equals"
    values: list[Any] = Field(default_factory=list)


class RCAAnalyzeRequest(BaseModel):
    metric: str
    time_dimension: str = "dt"
    current_start: str
    current_end: str
    baseline_start: str
    baseline_end: str
    dimensions: list[str] = Field(default_factory=list)
    filters: list[RCAFilterRequest] = Field(default_factory=list)
    limit: int = 20


@app.get("/health")
def health():
    stats = _vanna.gemini_stats if _vanna is not None else {}
    return {
        "status": "healthy",
        "doris": "lazy",
        "qwen_stats": stats,
        "gemini_stats": stats,
        "tracer_stats": tracer.stats(),
    }


@app.post("/ask")
def ask(req: AskRequest):
    result = get_vanna().ask_with_trace(req.question)
    return result


@app.post("/ask-lc")
def ask_lc(req: AskRequest):
    try:
        return get_ask_lc_pipeline().run_with_trace(
            req.question,
            prompt_version=req.prompt_version,
        )
    except Exception as e:
        logger.exception(f"[ask-lc] 失败: {e}")
        raise HTTPException(500, str(e))


@app.post("/ask/cube")
def ask_cube(req: AskRequest):
    try:
        result = get_cube_pipeline().run(req.question)
        if result.get("sql"):
            validate_readonly_sql(result["sql"])
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"[ask/cube] 失败: {e}")
        raise HTTPException(500, str(e))


@app.get("/ask/cube/stream")
def ask_cube_stream(q: str = Query(..., description="自然语言问题")):
    """
    Cube 路径 SSE 实时推送，逐步骤展示调用链路。
    事件类型: start | step_start | step_done | final | error
    """
    step_queue: queue_module.Queue = queue_module.Queue()

    def callback(event_type: str, data: dict):
        step_queue.put((event_type, data))

    def run_ask():
        try:
            get_cube_pipeline().run(q, step_callback=callback)
        except Exception as e:
            step_queue.put(("error", {"error": str(e)}))

    threading.Thread(target=run_ask, daemon=True).start()

    def event_generator():
        while True:
            try:
                event_type, data = step_queue.get(timeout=60)
                payload = json.dumps(data, ensure_ascii=False)
                yield f"event: {event_type}\ndata: {payload}\n\n"
                if event_type in ("final", "error"):
                    break
            except queue_module.Empty:
                yield ": keepalive\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.get("/rca/smart/stream")
def smart_rca_stream(q: str = Query(..., description="归因分析问题")):
    def event_generator():
        for event_type, data in get_smart_rca_pipeline().stream(q):
            if event_type == "keepalive":
                yield ": keepalive\n\n"
                continue
            payload = json.dumps(data, ensure_ascii=False)
            yield f"event: {event_type}\ndata: {payload}\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.post("/rca/smart")
def smart_rca(req: AskRequest):
    try:
        return get_smart_rca_pipeline().run(req.question)
    except Exception as e:
        logger.exception(f"[rca/smart] 失败: {e}")
        raise HTTPException(500, str(e))


@app.get("/cube/model/version")
def cube_model_version():
    try:
        get_cube_service().ensure_models()
        return get_cube_service().get_model_status()
    except Exception as e:
        logger.exception(f"[cube/model/version] 失败: {e}")
        raise HTTPException(500, str(e))


@app.post("/cube/reload-model")
def cube_reload_model():
    try:
        return get_cube_service().reload_models()
    except Exception as e:
        logger.exception(f"[cube/reload-model] 失败: {e}")
        raise HTTPException(500, str(e))


@app.post("/cube/generate_sql")
def cube_generate_sql(req: CubeGenerateSQLRequest):
    try:
        cube_query = CubeQuery(
            metrics=req.metrics,
            dimensions=req.dimensions,
            filters=[
                CubeFilter(
                    member=item.member,
                    operator=item.operator,
                    values=item.values,
                )
                for item in req.filters
            ],
            segments=req.segments,
            order=req.order,
            limit=req.limit,
            rag_hints=req.rag_hints,
        )
        result = get_cube_service().generate_sql(cube_query)
        if result.get("sql"):
            validate_readonly_sql(result["sql"])
        return result
    except Exception as e:
        logger.exception(f"[cube/generate_sql] 失败: {e}")
        raise HTTPException(500, str(e))


@app.get("/cube/admin/entities")
def cube_admin_entities():
    return {
        "entities": [
            {
                "name": name,
                "table": meta["table"],
                "pk": meta["pk"],
                "fields": meta["fields"],
            }
            for name, meta in CUBE_ADMIN_ENTITIES.items()
        ]
    }


@app.get("/cube/admin/{entity}")
def cube_admin_list(entity: str, limit: int = 300):
    meta = _cube_entity_meta(entity)
    limit = max(1, min(int(limit or 300), 1000))
    timestamp_field = meta.get("timestamp", "updated_at")
    fields = ", ".join(meta["fields"] + [timestamp_field])
    db_name = _cube_store_db()
    rows = get_cube_admin_client().execute(
        f"""
        SELECT {fields}
        FROM {db_name}.{meta['table']}
        ORDER BY {meta['order']}
        LIMIT {limit}
        """
    )
    return {
        "entity": entity,
        "table": meta["table"],
        "pk": meta["pk"],
        "fields": meta["fields"],
        "rows": rows,
    }


@app.post("/cube/admin/{entity}")
def cube_admin_upsert(entity: str, req: CubeAdminUpsertRequest):
    meta = _cube_entity_meta(entity)
    row = {k: v for k, v in (req.row or {}).items() if k in meta["fields"]}
    pk = meta["pk"]
    if pk not in row or row.get(pk) in ("", None):
        row[pk] = _stable_bigint(entity, json.dumps(row, ensure_ascii=False, sort_keys=True))
    if not row:
        raise HTTPException(400, "row 不能为空")
    columns = [col for col in meta["fields"] if col in row]
    placeholders = ", ".join(["%s"] * len(columns))
    values = [row[col] for col in columns]
    db_name = _cube_store_db()
    get_cube_admin_client().execute_write(
        f"""
        INSERT INTO {db_name}.{meta['table']} ({", ".join(columns)})
        VALUES ({placeholders})
        """,
        values,
    )
    return {"status": "ok", "entity": entity, "pk": pk, "id": row[pk]}


@app.delete("/cube/admin/{entity}/{row_id}")
def cube_admin_delete(entity: str, row_id: str):
    meta = _cube_entity_meta(entity)
    db_name = _cube_store_db()
    get_cube_admin_client().execute_write(
        f"DELETE FROM {db_name}.{meta['table']} WHERE {meta['pk']} = %s",
        (row_id,),
    )
    return {"status": "ok", "entity": entity, "deleted": row_id}


@app.post("/cube/admin/{entity}/sync-cache")
def cube_admin_sync_cache(entity: str):
    _cube_entity_meta(entity)
    return get_cube_service().reload_models()


@app.get("/cube/admin/dimension-values/collect-options")
def cube_collect_dimension_value_options():
    client = get_cube_admin_client()
    db_name = _cube_store_db()
    rows = client.execute(
        f"""
        SELECT d.cube_name, d.dimension_name, d.title, d.sql_expr,
               d.dimension_type, d.primary_key_flag, m.sql_table
        FROM {db_name}.cube_dimensions d
        JOIN {db_name}.cube_models m ON d.cube_name = m.cube_name
        WHERE d.visible = 1
          AND m.visible = 1
          AND d.dimension_type IN ('string', 'number', 'boolean')
        ORDER BY d.cube_name, d.dimension_name
        """
    )
    simple_col = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
    key_columns = _cube_primary_key_columns(rows, simple_col)
    label_columns = _cube_label_columns(rows, simple_col)
    options = []
    for row in rows:
        cube_name = row["cube_name"]
        dimension_name = row["dimension_name"]
        if not _enum_option_visible(cube_name, dimension_name):
            continue
        column = (row.get("sql_expr") or "").strip()
        table = (row.get("sql_table") or "").strip()
        eligible = bool(table and simple_col.match(column))
        code_column, label_column = _enum_collect_columns(
            cube_name,
            dimension_name,
            column,
            key_columns,
            label_columns,
        )
        group, group_title, group_order = _enum_business_group(cube_name, dimension_name)
        recommended = _enum_recommended(cube_name, dimension_name, eligible)
        reason = ""
        if not eligible:
            reason = "仅支持简单列名维度自动采集"
        elif not recommended:
            reason = "非推荐项，请确认低基数且会被自然语言提到"
        options.append({
            "key": f"{cube_name}.{dimension_name}",
            "cube_name": cube_name,
            "dimension_name": dimension_name,
            "title": _enum_option_title(cube_name, dimension_name, row.get("title") or ""),
            "sql_expr": column,
            "dimension_type": row.get("dimension_type") or "",
            "sql_table": table,
            "code_column": code_column,
            "label_column": label_column,
            "group": group,
            "group_title": group_title,
            "group_order": group_order,
            "eligible": eligible,
            "recommended": recommended,
            "reason": reason,
        })
    options.sort(key=lambda item: (
        item["group_order"],
        0 if item["recommended"] else 1,
        item["cube_name"],
        item["dimension_name"],
    ))
    return {"options": options}


@app.post("/cube/admin/dimension-values/collect")
def cube_collect_dimension_values(req: CubeEnumCollectRequest):
    client = get_cube_admin_client()
    max_values = max(1, min(int(req.max_values or 200), 1000))
    max_cardinality = max(1, min(int(req.max_cardinality or 500), 5000))
    include_cubes = {str(item).strip() for item in (req.include_cubes or []) if str(item).strip()}
    include_dimensions = {str(item).strip() for item in (req.include_dimensions or []) if str(item).strip()}
    exclude_cubes = {str(item).strip() for item in (req.exclude_cubes or []) if str(item).strip()}
    db_name = _cube_store_db()
    dims = client.execute(
        f"""
        SELECT d.cube_name, d.dimension_name, d.sql_expr, d.dimension_type,
               d.primary_key_flag, m.sql_table
        FROM {db_name}.cube_dimensions d
        JOIN {db_name}.cube_models m ON d.cube_name = m.cube_name
        WHERE d.visible = 1
          AND m.visible = 1
          AND d.dimension_type IN ('string', 'number', 'boolean')
        ORDER BY d.cube_name, d.dimension_name
        """
    )
    simple_col = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
    key_columns = _cube_primary_key_columns(dims, simple_col)
    label_columns = _cube_label_columns(dims, simple_col)
    inserted = 0
    skipped: list[dict[str, Any]] = []
    collected: list[dict[str, Any]] = []
    for dim in dims:
        cube_name = dim["cube_name"]
        dimension_name = dim["dimension_name"]
        column = (dim.get("sql_expr") or "").strip()
        table = (dim.get("sql_table") or "").strip()
        dimension_key = f"{cube_name}.{dimension_name}"
        if include_dimensions and dimension_key not in include_dimensions:
            skipped.append({"dimension": dimension_key, "reason": "未勾选采集"})
            continue
        if include_cubes and cube_name not in include_cubes:
            skipped.append({"dimension": dimension_key, "reason": "Cube 未勾选采集"})
            continue
        explicitly_selected = dimension_key in include_dimensions or cube_name in include_cubes
        if cube_name in exclude_cubes and not explicitly_selected:
            skipped.append({"dimension": dimension_key, "reason": "明细/事实 Cube 默认不自动采集枚举"})
            continue
        if not table or not simple_col.match(column):
            skipped.append({"dimension": dimension_key, "reason": "sql_expr 非简单列名"})
            continue
        code_column, label_column = _enum_collect_columns(
            cube_name,
            dimension_name,
            column,
            key_columns,
            label_columns,
        )
        rows = client.execute(
            f"""
            SELECT
              CAST({code_column} AS STRING) AS value_code,
              CAST({label_column} AS STRING) AS value_label,
              COUNT(*) AS usage_count
            FROM {table}
            WHERE {code_column} IS NOT NULL
              AND {label_column} IS NOT NULL
            GROUP BY {code_column}, {label_column}
            ORDER BY usage_count DESC
            LIMIT {max_cardinality + 1}
            """
        )
        if len(rows) > max_cardinality:
            skipped.append({"dimension": dimension_key, "reason": f"基数超过 {max_cardinality}"})
            continue
        count = 0
        for row in rows[:max_values]:
            value_code = str(row.get("value_code") or "").strip()
            value_label = str(row.get("value_label") or value_code).strip()
            if not value_code:
                continue
            value_id = _stable_bigint(cube_name, dimension_name, value_code)
            usage_count = int(row.get("usage_count", 0) or 0)
            client.execute_write(
                f"""
                INSERT INTO {db_name}.cube_dimension_values
                  (value_id, cube_name, dimension_name, value_code, value_label,
                   aliases_json, source, source_table, source_column, usage_count, visible, version)
                VALUES (%s, %s, %s, %s, %s, %s, 'scan', %s, %s, %s, 1, 1)
                """,
                (
                    value_id,
                    cube_name,
                    dimension_name,
                    value_code,
                    value_label,
                    "[]",
                    table,
                    label_column,
                    usage_count,
                ),
            )
            inserted += 1
            count += 1
        collected.append({"dimension": dimension_key, "count": count})
    manifest = get_cube_service().reload_models()
    return {
        "status": "ok",
        "inserted": inserted,
        "collected": collected,
        "skipped": skipped,
        "model": manifest,
    }


@app.post("/cube/validate")
def cube_validate(req: CubeValidateRequest):
    try:
        return get_cube_validator().validate(
            persist=req.persist,
            explain_sql=req.explain_sql,
        )
    except Exception as e:
        logger.exception(f"[cube/validate] 失败: {e}")
        raise HTTPException(500, str(e))


@app.get("/cube/validate/latest")
def cube_validate_latest(limit: int = 200):
    try:
        return get_cube_validator().latest(limit=limit)
    except Exception as e:
        logger.exception(f"[cube/validate/latest] 失败: {e}")
        raise HTTPException(500, str(e))


@app.get("/rca/options")
def rca_options():
    try:
        return get_rca_service().options()
    except Exception as e:
        logger.exception(f"[rca/options] 失败: {e}")
        raise HTTPException(500, str(e))


@app.get("/rca/graph")
def rca_graph(metric: Optional[str] = Query(None, description="可选目标指标，如 gmv")):
    try:
        service = get_rca_service()
        if metric:
            return service.metric_graph(metric)
        return service.graph_summary()
    except Exception as e:
        logger.exception(f"[rca/graph] 失败: {e}")
        raise HTTPException(500, str(e))


def _json_obj(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default


def _rca_run_payload(row: dict[str, Any]) -> dict[str, Any]:
    plan = _json_obj(row.get("plan_json"), {})
    result = _json_obj(row.get("causal_results_json"), {})
    candidates = _json_obj(row.get("candidates_json"), [])
    steps = plan.get("steps") if isinstance(plan, dict) else []
    total_ms = 0.0
    if isinstance(steps, list):
        total_ms = sum(float(item.get("duration_ms", 0) or 0) for item in steps if isinstance(item, dict))
    return {
        "run_id": row.get("run_id"),
        "question": row.get("question"),
        "metric_name": row.get("metric_name"),
        "status": row.get("status"),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
        "plan": plan,
        "steps": steps if isinstance(steps, list) else [],
        "candidates": candidates if isinstance(candidates, list) else [],
        "result": result,
        "summary": row.get("report_text") or "",
        "total_ms": total_ms,
    }


@app.get("/rca/runs")
def rca_runs(limit: int = 30):
    limit = max(1, min(int(limit or 30), 200))
    db_name = _rca_store_db()
    try:
        rows = get_rca_admin_client().execute(
            f"""
            SELECT run_id, question, metric_name, status, plan_json,
                   candidates_json, causal_results_json, report_text,
                   created_at, updated_at
            FROM {db_name}.rca_runs
            ORDER BY created_at DESC
            LIMIT {limit}
            """
        )
        runs = [_rca_run_payload(row) for row in rows]
        ok = sum(1 for item in runs if item.get("status") == "ok")
        total = len(runs)
        avg_ms = round(sum(float(item.get("total_ms") or 0) for item in runs) / total, 1) if total else 0
        return {
            "runs": runs,
            "stats": {
                "total": total,
                "ok": ok,
                "error": total - ok,
                "success_rate": f"{ok / total * 100:.1f}%" if total else "—",
                "avg_ms": avg_ms,
            },
        }
    except Exception as e:
        logger.exception(f"[rca/runs] 失败: {e}")
        raise HTTPException(500, str(e))


@app.get("/rca/runs/{run_id}")
def rca_run_detail(run_id: str):
    db_name = _rca_store_db()
    try:
        rows = get_rca_admin_client().execute(
            f"""
            SELECT run_id, question, metric_name, status, plan_json,
                   candidates_json, causal_results_json, report_text,
                   created_at, updated_at
            FROM {db_name}.rca_runs
            WHERE run_id = %s
            LIMIT 1
            """,
            (run_id,),
        )
        if not rows:
            raise HTTPException(404, "RCA run not found")
        candidates = get_rca_admin_client().execute(
            f"""
            SELECT candidate_id, run_id, rank_no, candidate_type, candidate_json,
                   runtime_contribution, prior_score, causal_score, final_score, created_at
            FROM {db_name}.rca_run_candidates
            WHERE run_id = %s
            ORDER BY rank_no
            LIMIT 200
            """,
            (run_id,),
        )
        payload = _rca_run_payload(rows[0])
        payload["candidate_rows"] = [
            {**row, "candidate": _json_obj(row.get("candidate_json"), {})}
            for row in candidates
        ]
        return payload
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"[rca/runs/detail] 失败: {e}")
        raise HTTPException(500, str(e))


@app.get("/rca/admin/entities")
def rca_admin_entities():
    return {
        "entities": [
            {
                "name": name,
                "table": meta["table"],
                "pk": meta["pk"],
                "fields": meta["fields"],
                "readonly": bool(meta.get("readonly", False)),
            }
            for name, meta in RCA_ADMIN_ENTITIES.items()
        ]
    }


@app.get("/rca/admin/{entity}")
def rca_admin_list(entity: str, limit: int = 300):
    meta = _rca_entity_meta(entity)
    limit = max(1, min(int(limit or 300), 1000))
    timestamp_field = meta.get("timestamp", "updated_at")
    fields = ", ".join(meta["fields"] + [timestamp_field])
    db_name = _rca_store_db()
    rows = get_rca_admin_client().execute(
        f"""
        SELECT {fields}
        FROM {db_name}.{meta['table']}
        ORDER BY {meta['order']}
        LIMIT {limit}
        """
    )
    return {
        "entity": entity,
        "table": meta["table"],
        "pk": meta["pk"],
        "fields": meta["fields"],
        "readonly": bool(meta.get("readonly", False)),
        "rows": rows,
    }


@app.post("/rca/admin/{entity}")
def rca_admin_upsert(entity: str, req: CubeAdminUpsertRequest):
    meta = _rca_entity_meta(entity)
    if meta.get("readonly"):
        raise HTTPException(400, f"{entity} 为运行记录，只读")
    row = {k: v for k, v in (req.row or {}).items() if k in meta["fields"]}
    pk = meta["pk"]
    if pk not in row or row.get(pk) in ("", None):
        row[pk] = _stable_bigint("rca", entity, json.dumps(row, ensure_ascii=False, sort_keys=True))
    if not row:
        raise HTTPException(400, "row 不能为空")
    columns = [col for col in meta["fields"] if col in row]
    placeholders = ", ".join(["%s"] * len(columns))
    values = [row[col] for col in columns]
    db_name = _rca_store_db()
    get_rca_admin_client().execute_write(
        f"""
        INSERT INTO {db_name}.{meta['table']} ({", ".join(columns)})
        VALUES ({placeholders})
        """,
        values,
    )
    global _rca_service, _smart_rca_pipeline
    _rca_service = None
    _smart_rca_pipeline = None
    return {"status": "ok", "entity": entity, "pk": pk, "id": row[pk]}


@app.delete("/rca/admin/{entity}/{row_id}")
def rca_admin_delete(entity: str, row_id: str):
    meta = _rca_entity_meta(entity)
    if meta.get("readonly"):
        raise HTTPException(400, f"{entity} 为运行记录，只读")
    db_name = _rca_store_db()
    get_rca_admin_client().execute_write(
        f"DELETE FROM {db_name}.{meta['table']} WHERE {meta['pk']} = %s",
        (row_id,),
    )
    global _rca_service, _smart_rca_pipeline
    _rca_service = None
    _smart_rca_pipeline = None
    return {"status": "ok", "entity": entity, "deleted": row_id}


@app.post("/rca/analyze")
def rca_analyze(req: RCAAnalyzeRequest):
    try:
        from vanna_skill.rca.service import RCARequest

        return get_rca_service().analyze(
            RCARequest(
                metric=req.metric,
                time_dimension=req.time_dimension,
                current_start=req.current_start,
                current_end=req.current_end,
                baseline_start=req.baseline_start,
                baseline_end=req.baseline_end,
                dimensions=req.dimensions,
                filters=[
                    CubeFilter(member=item.member, operator=item.operator, values=item.values)
                    for item in req.filters
                ],
                limit=req.limit,
            )
        )
    except Exception as e:
        logger.exception(f"[rca/analyze] 失败: {e}")
        raise HTTPException(500, str(e))


@app.post("/execute")
def execute_sql(body: dict):
    """执行 SQL 并返回结果"""
    sql = body.get("sql", "")
    validate_readonly_sql(sql)
    try:
        logger.info("[execute] 执行 SQL:\n%s", sql)
        df = get_execute_client().query_df(sql)
        return {
            "columns": df.columns.tolist(),
            "rows": df.head(200).values.tolist(),
            "total": len(df),
        }
    except Exception as e:
        raise HTTPException(500, str(e))


# ── 训练数据 CRUD ─────────────────────────────────────────────────────────────

@app.get("/training-data")
def get_training_data(content_type: str = "", limit: int = 200):
    vn = get_vanna()
    df = vn.get_training_data()
    if content_type:
        df = df[df["content_type"] == content_type]
    page_df = df.head(limit)
    clean_df = page_df.astype(object).where(pd.notna(page_df), None)
    return {"data": clean_df.to_dict(orient="records"), "total": len(df)}


def _clean_df(df):
    page_df = df.astype(object).where(pd.notna(df), None)
    return page_df.to_dict(orient="records")


@app.get("/sources/sql")
def get_sql_sources(limit: int = 300):
    df = get_vanna().get_sql_source_data(limit=limit)
    return {"data": _clean_df(df), "total": len(df)}


@app.get("/sources/doc")
def get_doc_sources(limit: int = 300):
    df = get_vanna().get_doc_source_data(limit=limit)
    return {"data": _clean_df(df), "total": len(df)}


@app.get("/sources/metadata")
def get_metadata_sources(limit: int = 300):
    df = get_vanna().get_metadata_source_data(limit=limit)
    return {"data": _clean_df(df), "total": len(df)}


@app.get("/sources/lineage")
def get_lineage_sources(limit: int = 300):
    df = get_vanna().get_lineage_source_data(limit=limit)
    return {"data": _clean_df(df), "total": len(df)}


@app.post("/training-data/sql")
def add_sql(req: TrainSqlRequest):
    validate_readonly_sql(req.sql)
    get_vanna().add_question_sql(req.question, req.sql, source=req.source)
    return {"status": "ok"}


@app.post("/training-data/ddl")
def add_ddl(req: TrainDdlRequest):
    get_vanna().add_ddl(req.ddl, source=req.source)
    return {"status": "ok"}


@app.post("/training-data/doc")
def add_doc(req: TrainDocRequest):
    get_vanna().add_documentation(req.documentation, source=req.source)
    return {"status": "ok"}


@app.delete("/training-data/{id}")
def delete_training(id: str):
    ok = get_vanna().remove_training_data(id)
    return {"status": "ok" if ok else "not_found"}


# ── 元数据 ────────────────────────────────────────────────────────────────────

@app.get("/metadata/tables")
def meta_tables():
    from vanna_skill import MetadataManager
    meta = MetadataManager(get_vanna()._biz, CONFIG["database"])
    df = meta.to_dataframe()
    return {"tables": df.to_dict(orient="records")}


@app.get("/metadata/tables/{table_name}")
def meta_table_detail(table_name: str):
    from vanna_skill import MetadataManager
    meta = MetadataManager(get_vanna()._biz, CONFIG["database"])
    m = meta.get_table(table_name)
    if not m:
        raise HTTPException(404, f"Table {table_name} not found")
    return {
        "table_name": m.table_name, "comment": m.table_comment,
        "engine": m.engine, "rows": m.table_rows,
        "ddl": m.to_ddl(),
        "columns": [{"name": c.column_name, "type": c.data_type,
                     "comment": c.column_comment, "nullable": c.is_nullable}
                    for c in m.columns],
    }


@app.post("/metadata/sync")
def meta_sync():
    from vanna_skill import MetadataManager
    vn = get_vanna()
    meta = MetadataManager(vn._biz, CONFIG["database"])
    tables = meta.sync()
    ddl_added = 0
    ddl_skipped = 0
    doc_added = 0
    doc_skipped = 0
    meta_added = 0
    meta_skipped = 0
    for m in tables:
        meta_result = vn.add_metadata(
            table_name=m.table_name,
            ddl=m.to_ddl(),
            summary=m.summary(),
            source="schema",
            table_comment=m.table_comment,
            engine=m.engine,
            table_rows=m.table_rows,
        )
        meta_added += 1 if meta_result["status"] == "ok" else 0
        meta_skipped += 1 if meta_result["status"] == "exists" else 0
        ddl_added += 1 if meta_result["ddl"] == "ok" else 0
        ddl_skipped += 1 if meta_result["ddl"] == "exists" else 0
        doc_added += 1 if meta_result["doc"] == "ok" else 0
        doc_skipped += 1 if meta_result["doc"] == "exists" else 0
    return {
        "status": "ok",
        "tables_synced": len(tables),
        "metadata_added": meta_added,
        "metadata_skipped": meta_skipped,
        "ddl_added": ddl_added,
        "ddl_skipped": ddl_skipped,
        "doc_added": doc_added,
        "doc_skipped": doc_skipped,
    }


# ── 审计日志挖掘 ──────────────────────────────────────────────────────────────

@app.post("/audit/mine")
def mine_audit(limit: int = 500, max_ms: int = 30000):
    from vanna_skill import AuditMiner
    vn = get_vanna()
    miner = AuditMiner(vn._biz, vn)
    return miner.mine(max_query_time_ms=max_ms, limit=limit)


@app.get("/lineage")
def get_lineage(limit: int = 300):
    rows = get_vanna()._vec.execute(
        f"""
        SELECT edge_id, source_table, target_table, relation_type,
               sql_type, source, freq, created_at
        FROM vanna_store.vanna_lineage
        ORDER BY freq DESC, created_at DESC
        LIMIT {int(limit)}
        """
    )
    return {"data": rows, "total": len(rows)}


@app.post("/lineage/rebuild")
def rebuild_lineage():
    """重建 vanna_lineage，并刷新进程级血缘缓存。"""
    vn = get_vanna()
    manager = LineageManager(vn._biz, vn._vec)
    result = manager.rebuild_lineage_table()
    invalidate_lineage_cache()
    logger.info("[Lineage] rebuild result=%s", result)
    return {"status": "ok", **result}


# ── 用户反馈 ──────────────────────────────────────────────────────────────────

@app.post("/feedback")
def feedback(req: FeedbackRequest):
    vn = get_vanna()
    rag_enabled_engines = {"semantic", "cube"}

    def _upsert_sql_rag(question: str, sql: str, source: str) -> str:
        if req.engine not in rag_enabled_engines:
            return ""
        semantic_client = DorisClient(
            host=CONFIG["host"],
            port=CONFIG["port"],
            user=CONFIG["user"],
            password=CONFIG.get("password", ""),
            database="semantic_store",
        )
        store = SemanticSQLRAGStore(
            semantic_client,
            semantic_client,
            get_cube_pipeline()._llm if req.engine == "cube" else get_semantic_pipeline()._llm,
            db_name=CONFIG.get("database", "retail_dw"),
        )
        return store.upsert_feedback_sample(question, sql, source=source)

    if req.is_correct:
        validate_readonly_sql(req.sql)
        vn.add_question_sql(req.question, req.sql, source="feedback")
        semantic_action = _upsert_sql_rag(req.question, req.sql, "feedback")
        return {"status": "ok", "action": "added", "semantic_rag_action": semantic_action}
    elif req.corrected_sql:
        validate_readonly_sql(req.corrected_sql)
        vn.add_question_sql(req.question, req.corrected_sql,
                            source="feedback_corrected")
        semantic_action = _upsert_sql_rag(req.question, req.corrected_sql, "feedback_corrected")
        return {"status": "ok", "action": "corrected", "semantic_rag_action": semantic_action}
    return {"status": "ok", "action": "negative_recorded"}


# ── 调用链路日志 ──────────────────────────────────────────────────────────────

@app.get("/traces")
def get_traces(n: int = 30):
    vn = get_vanna()
    n = max(1, min(int(n or 30), 30))
    try:
        traces = vn.get_trace_logs(n)
    except Exception:
        traces = [t.to_dict() for t in tracer.recent(n)]
    return {"traces": traces, "stats": tracer.stats()}


@app.get("/traces/{trace_id}")
def get_trace(trace_id: str):
    vn = get_vanna()
    try:
        trace = vn.get_trace_log(trace_id)
        if trace:
            return trace
    except Exception:
        pass
    t = tracer.get(trace_id)
    if not t:
        raise HTTPException(404, "Trace not found")
    return t.to_dict()


# ── 配置管理 ──────────────────────────────────────────────────────────────────

@app.get("/config")
def get_config():
    safe = {k: v for k, v in CONFIG.items() if k != "qwen_api_key"}
    safe["qwen_api_key"] = ""
    safe["qwen_api_key_masked"] = (
        CONFIG["qwen_api_key"][:8] + "..." if CONFIG["qwen_api_key"] else ""
    )
    return safe


@app.post("/config")
def update_config(req: ConfigRequest):
    global CONFIG, _vanna, _ask_lc_pipeline, _semantic_pipeline, _prompt_store, _cube_service, _cube_pipeline, _cube_admin_client, _cube_validator, _rca_service, _smart_rca_pipeline, _rca_admin_client, _execute_client
    qwen_api_key = req.qwen_api_key.strip()
    updated = CONFIG.copy()
    updated.update({
        "host": req.host, "port": req.port,
        "user": req.user, "password": req.password,
        "database": req.database,
        "model": req.model,
        "n_results": req.n_results,
        "langchain_fallback_enabled": req.langchain_fallback_enabled,
        "embedding_fallback_mode": "keyword" if req.langchain_fallback_enabled else "fail",
        "semantic_to_langchain_fallback_enabled": req.semantic_to_langchain_fallback_enabled,
        "semantic_sql_rag_enabled": req.semantic_sql_rag_enabled,
        "cube_store_database": req.cube_store_database.strip() or "cube_store",
        "rca_store_database": req.rca_store_database.strip() or "rca_store",
        "cube_model_reload_each_request": req.cube_model_reload_each_request,
        "cube_default_time_scope": req.cube_default_time_scope.strip(),
    })
    if qwen_api_key and "..." not in qwen_api_key:
        updated["qwen_api_key"] = qwen_api_key
    CONFIG = save_config(updated)
    _vanna = None  # 重置连接，下次调用重新初始化
    _ask_lc_pipeline = None
    _semantic_pipeline = None
    _prompt_store = None
    _cube_service = None
    _cube_pipeline = None
    _cube_admin_client = None
    _cube_validator = None
    _rca_service = None
    _smart_rca_pipeline = None
    _rca_admin_client = None
    _execute_client = None
    invalidate_semantic_cache()
    return {"status": "ok", "message": "配置已更新，连接已重置"}


@app.get("/system-prompt")
def get_system_prompt():
    state = get_prompt_store().get_prompt_state(CONFIG.get("initial_prompt", ""))
    default = next((item for item in state["prompt_versions"] if item["id"] == "default"), None)
    return {"initial_prompt": (default or {}).get("system_prompt", "")}


@app.post("/system-prompt")
def update_system_prompt(req: SystemPromptRequest):
    global CONFIG, _vanna, _ask_lc_pipeline
    updated = CONFIG.copy()
    updated["initial_prompt"] = req.initial_prompt
    CONFIG = save_config(updated)
    db_msg = ""
    try:
        store = get_prompt_store()
        store.save_prompt_version({
            "id": "default",
            "name": "Default",
            "description": "当前主提示词版本",
            "system_prompt": req.initial_prompt,
        })
        store.activate_prompt("default")
    except Exception as exc:
        db_msg = f"（Prompt DB 写入失败，已保存至配置文件: {exc}）"
        logger.warning(f"[system-prompt] DB 写入失败: {exc}")
    _vanna = None
    _ask_lc_pipeline = None
    return {"status": "ok", "message": f"System Prompt 已保存并生效 {db_msg}".strip()}


@app.get("/prompt-versions")
def get_prompt_versions():
    # get_prompt_state 内部已有 fallback，此处不会抛 500
    return get_prompt_store().get_prompt_state(CONFIG.get("initial_prompt", ""))


@app.post("/prompt-versions/save")
def save_prompt_version(req: PromptVersionRequest):
    global CONFIG, _ask_lc_pipeline
    version_id = req.id.strip()
    if not version_id:
        raise HTTPException(400, "版本 ID 不能为空")
    try:
        get_prompt_store().save_prompt_version({
            "id": version_id,
            "name": req.name.strip() or version_id,
            "description": req.description,
            "system_prompt": req.system_prompt,
        })
    except Exception as exc:
        raise HTTPException(500, f"Prompt DB 写入失败: {exc}")
    updated = CONFIG.copy()
    if version_id == "default":
        updated["initial_prompt"] = req.system_prompt
    CONFIG = save_config(updated)
    _ask_lc_pipeline = None
    state = get_prompt_store().get_prompt_state(CONFIG.get("initial_prompt", ""))
    return {
        "status": "ok",
        "message": f"Prompt 版本 {version_id} 已保存",
        "prompt_versions": state["prompt_versions"],
        "active_prompt_version": state["active_prompt_version"],
    }


@app.post("/prompt-versions/activate")
def activate_prompt_version(req: PromptActivateRequest):
    global _ask_lc_pipeline
    version_id = req.version_id.strip()
    try:
        versions = get_prompt_store().list_prompt_versions()
        if version_id not in {item.get("id") for item in versions}:
            raise HTTPException(404, f"Prompt 版本不存在: {version_id}")
        get_prompt_store().activate_prompt(version_id)
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(500, f"Prompt DB 操作失败: {exc}")
    _ask_lc_pipeline = None
    return {"status": "ok", "message": f"已激活版本 {version_id}"}


@app.post("/prompt-versions/ab-test")
def update_ab_test(req: ABTestRequest):
    global _ask_lc_pipeline
    try:
        versions = {item.get("id") for item in get_prompt_store().list_prompt_versions()}
    except Exception as exc:
        raise HTTPException(500, f"Prompt DB 读取失败: {exc}")
    if req.version_a not in versions:
        raise HTTPException(404, f"版本不存在: {req.version_a}")
    if req.version_b and req.version_b not in versions:
        raise HTTPException(404, f"版本不存在: {req.version_b}")
    if req.enabled and not req.version_b:
        raise HTTPException(400, "启用 A/B Test 时必须提供 B 版本")
    try:
        get_prompt_store().save_ab_test(req.enabled, req.version_a, req.version_b)
    except Exception as exc:
        raise HTTPException(500, f"Prompt DB 写入失败: {exc}")
    _ask_lc_pipeline = None
    state = get_prompt_store().get_prompt_state(CONFIG.get("initial_prompt", ""))
    return {"status": "ok", "message": "A/B Test 配置已保存", "ab_test": state.get("ab_test", {})}


# ════════════════════════════════════════════════════════════════════════════
# 语义路径 API
# ════════════════════════════════════════════════════════════════════════════

@app.post("/ask/semantic")
def ask_semantic(req: AskRequest):
    """语义路径同步接口（POST）。"""
    try:
        return get_semantic_pipeline().run(req.question)
    except Exception as e:
        logger.exception(f"[ask/semantic] 失败: {e}")
        raise HTTPException(500, str(e))


@app.get("/ask/semantic/stream")
def ask_semantic_stream(q: str = Query(..., description="自然语言问题")):
    """
    语义路径 SSE 实时推送。
    事件类型与 /ask-lc/stream 相同：start | step_start | step_done | final | error
    """
    step_queue: queue_module.Queue = queue_module.Queue()

    def callback(event_type: str, data: dict):
        step_queue.put((event_type, data))

    def run_ask():
        try:
            get_semantic_pipeline().run(q, step_callback=callback)
        except Exception as e:
            step_queue.put(("error", {"error": str(e)}))

    threading.Thread(target=run_ask, daemon=True).start()

    def event_generator():
        while True:
            try:
                event_type, data = step_queue.get(timeout=60)
                payload = json.dumps(data, ensure_ascii=False)
                yield f"event: {event_type}\ndata: {payload}\n\n"
                if event_type in ("final", "error"):
                    break
            except queue_module.Empty:
                yield ": keepalive\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.post("/semantic/reload")
def semantic_reload():
    """强制从 YAML 重新加载语义目录（YAML → DB），并刷新进程级缓存。"""
    global _semantic_pipeline
    try:
        pipeline = get_semantic_pipeline()
        pipeline._catalog.reload()
        invalidate_semantic_cache()
        _semantic_pipeline = None   # 下次请求重建（新 catalog 实例）
        stats = pipeline._catalog.stats()
        return {"status": "ok", "message": "已从 YAML 重新加载并写入 DB", "stats": stats}
    except Exception as e:
        logger.exception(f"[semantic/reload] 失败: {e}")
        raise HTTPException(500, str(e))


@app.post("/semantic/cache/refresh")
def semantic_cache_refresh():
    """从 semantic_store 重新加载语义目录到内存，不改写 DB/YAML。"""
    global _semantic_pipeline
    try:
        invalidate_semantic_cache()
        _semantic_pipeline = None
        pipeline = get_semantic_pipeline()
        stats = pipeline._catalog.stats()
        logger.info("[semantic/cache/refresh] 已从 DB 刷新语义内存: %s", stats)
        return {
            "status": "ok",
            "message": "已从 semantic_store 刷新语义内存",
            "stats": stats,
        }
    except Exception as e:
        logger.exception(f"[semantic/cache/refresh] 失败: {e}")
        raise HTTPException(500, str(e))


@app.get("/semantic/export")
def semantic_export():
    """
    DB → YAML 导出：读取 semantic_store 当前状态，返回 YAML 文本。
    前端可用此接口下载当前语义定义，编辑后再通过 /semantic/import 导入。
    """
    try:
        catalog = get_semantic_pipeline()._catalog
        yaml_str = catalog.dump_yaml()
        from fastapi.responses import Response
        return Response(
            content=yaml_str,
            media_type="text/yaml; charset=utf-8",
            headers={"Content-Disposition": f"attachment; filename=semantic_{catalog._db_name}.yaml"},
        )
    except Exception as e:
        raise HTTPException(500, str(e))


class ImportYamlRequest(BaseModel):
    yaml_content: str = Field(..., description="YAML 文本内容")
    save_file: bool = Field(False, description="是否同时覆写本地 YAML 文件")


@app.post("/semantic/import")
def semantic_import(req: ImportYamlRequest):
    """
    YAML → DB 导入：把 YAML 文本解析后写入 semantic_store，刷新内存 Catalog。
    这是维护语义定义的主入口：在 UI 里编辑 YAML → 点「导入」→ 立即生效。
    """
    global _semantic_pipeline
    try:
        catalog = get_semantic_pipeline()._catalog
        stats = catalog.import_yaml(req.yaml_content, save_to_db=True)
        if req.save_file:
            path = catalog.save_yaml_file()
            return {"status": "ok", "message": f"已导入并保存到 {path}", "stats": stats}
        # 刷新全局缓存
        invalidate_semantic_cache()
        _semantic_pipeline = None
        return {"status": "ok", "message": "YAML 已导入 DB，语义目录已刷新", "stats": stats}
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        logger.exception(f"[semantic/import] 失败: {e}")
        raise HTTPException(500, str(e))


class UpsertMetricRequest(BaseModel):
    name: str
    label: str
    metric_type: str = "simple"
    complexity: str = "normal"
    expression: str = ""
    primary_source: Optional[dict] = None
    extra_joins: list = []
    time_column: str = ""
    numerator_expr: str = ""
    denominator_expr: str = ""
    output_format: str = "number"
    unit: str = ""
    compatible_dimensions: list = []
    synonyms: list = []
    tags: list = []
    description: str = ""


class UpsertDimensionRequest(BaseModel):
    name: str
    label: str
    dim_type: str = "attribute"
    entity: Optional[str] = None
    grain: Optional[str] = None
    expression: str = ""
    alias: str = ""
    join: Optional[dict] = None
    select_fields: list = []
    synonyms: list = []
    tags: list = []
    description: str = ""


@app.put("/semantic/metric")
def upsert_metric(req: UpsertMetricRequest):
    """单条指标 upsert（新增或覆盖），立即写入 DB 并刷新内存 Catalog。"""
    try:
        catalog = get_semantic_pipeline()._catalog
        catalog.upsert_metric(req.dict())
        return {"status": "ok", "name": req.name}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.put("/semantic/dimension")
def upsert_dimension(req: UpsertDimensionRequest):
    """单条维度 upsert（新增或覆盖），立即写入 DB 并刷新内存 Catalog。"""
    try:
        catalog = get_semantic_pipeline()._catalog
        catalog.upsert_dimension(req.dict())
        return {"status": "ok", "name": req.name}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.delete("/semantic/node/{node_type}/{name}")
def delete_semantic_node(node_type: str, name: str):
    """
    删除单个语义节点。
    node_type: metric | dimension | entity | business
    """
    try:
        catalog = get_semantic_pipeline()._catalog
        ok = catalog.delete_node(node_type, name)
        if not ok:
            raise HTTPException(404, f"{node_type}:{name} 不存在")
        return {"status": "ok", "deleted": f"{node_type}:{name}"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/semantic/catalog")
def semantic_catalog():
    """返回当前语义目录统计信息及节点列表（用于管理页面）。"""
    try:
        catalog = get_semantic_pipeline()._catalog
        catalog.refresh_from_db(required=False)
        return {
            "stats": catalog.stats(),
            "metrics": [
                {
                    "name": m.name,
                    "label": m.label,
                    "metric_type": m.metric_type,
                    "complexity": m.complexity,
                    "expression": m.expression,
                    "primary_table": m.primary_table,
                    "primary_alias": m.primary_alias,
                    "time_column": m.time_column,
                    "extra_joins": [
                        {"table": j.table, "alias": j.alias, "join_type": j.join_type, "on": j.on}
                        for j in m.extra_joins
                    ],
                    "compatible_dimensions": m.compatible_dimensions,
                    "output_format": m.output_format,
                    "unit": m.unit,
                    "tags": m.tags,
                    "synonyms": m.synonyms,
                    "description": m.description,
                }
                for m in catalog._metrics.values()
            ],
            "dimensions": [
                {
                    "name": d.name,
                    "label": d.label,
                    "dim_type": d.dim_type,
                    "grain": d.grain,
                    "expression": d.expression,
                    "alias": d.alias,
                    "join": {
                        "table": d.join.table,
                        "alias": d.join.alias,
                        "join_type": d.join.join_type,
                        "on": d.join.on,
                    } if d.join else None,
                    "select_fields": d.select_fields,
                    "tags": d.tags,
                    "synonyms": d.synonyms,
                    "description": d.description,
                }
                for d in catalog._dimensions.values()
            ],
            "business_domains": [
                {
                    "name": b.name,
                    "label": b.label,
                    "related_metrics": b.related_metrics,
                    "related_dimensions": b.related_dimensions,
                    "typical_questions": b.typical_questions,
                    "synonyms": b.synonyms,
                }
                for b in catalog._businesses.values()
            ],
        }
    except Exception as e:
        raise HTTPException(500, str(e))


class ScanRequest(BaseModel):
    include_tables: Optional[list] = None   # None = 全部表
    audit_limit: int = 5000                 # 分析最近 N 条 audit_log
    min_confidence: float = 0.3             # 过滤低置信度草稿
    apply_to_db: bool = False               # True = 扫描后直接写入 DB（跳过预览）


@app.post("/semantic/scan")
def semantic_scan(req: ScanRequest):
    """
    自动扫描 information_schema + audit_log，生成语义定义草稿。

    默认只返回草稿（apply_to_db=false），前端预览后用户选择性应用。
    apply_to_db=true 时直接写入 semantic_store（适合首次自动初始化）。
    """
    try:
        cfg = build_runtime_config()
        conn = dict(
            host=cfg["host"], port=cfg["port"],
            user=cfg["user"], password=cfg.get("password", ""),
        )
        db_name = cfg.get("database", "retail_dw")

        # biz_client：用于 information_schema 查询（不指定 database，information_schema 是全局的）
        biz_client = DorisClient(**conn, database=db_name)
        # audit_client：同一个连接即可，audit_log 在 __internal_schema
        audit_client = DorisClient(**conn, database="")

        scanner = SchemaScanner(
            biz_client=biz_client,
            audit_client=audit_client,
            db_name=db_name,
        )
        result = scanner.scan(
            include_tables=req.include_tables,
            audit_limit=req.audit_limit,
            min_confidence=req.min_confidence,
        )

        proposals_out = [
            {
                "node_type": p.node_type,
                "name": p.name,
                "label": p.label,
                "description": p.description,
                "confidence": round(p.confidence, 2),
                "source": p.source,
                "data": p.data,
            }
            for p in result.proposals
        ]

        response = {
            "status": "ok",
            "db_name": result.db_name,
            "stats": {
                "tables_scanned": result.tables_scanned,
                "columns_scanned": result.columns_scanned,
                "audit_logs_analyzed": result.audit_logs_analyzed,
                "proposals_total": len(result.proposals),
                "by_type": {
                    "entity":    sum(1 for p in result.proposals if p.node_type == "entity"),
                    "dimension": sum(1 for p in result.proposals if p.node_type == "dimension"),
                    "metric":    sum(1 for p in result.proposals if p.node_type == "metric"),
                },
            },
            "proposals": proposals_out,
            "warnings": result.warnings,
        }

        if req.apply_to_db:
            # 直接把全部草稿写入 semantic_store
            import yaml as _yaml
            yaml_dict = result.to_yaml_dict()
            catalog = get_semantic_pipeline()._catalog
            stats = catalog.import_yaml(
                _yaml.dump(yaml_dict, allow_unicode=True, default_flow_style=False),
                save_to_db=True,
            )
            invalidate_semantic_cache()
            global _semantic_pipeline
            _semantic_pipeline = None
            response["applied"] = True
            response["catalog_stats"] = stats
        else:
            response["applied"] = False

        return response

    except Exception as e:
        logger.exception(f"[semantic/scan] 失败: {e}")
        raise HTTPException(500, str(e))


class ApplyScanRequest(BaseModel):
    proposals: list   # 前端选中的草稿 data 列表（每项是 YAML-compatible dict）
    node_types: Optional[list] = None  # 可选过滤：["metric","dimension","entity"]


@app.post("/semantic/scan/apply")
def semantic_scan_apply(req: ApplyScanRequest):
    """
    把扫描草稿中用户选中的部分写入 semantic_store。
    前端在预览页勾选条目后调用此接口。
    """
    try:
        import yaml as _yaml
        catalog = get_semantic_pipeline()._catalog

        entities, dimensions, metrics = [], [], []
        for item in req.proposals:
            nt = item.get("node_type", "")
            if req.node_types and nt not in req.node_types:
                continue
            d = item.get("data", item)
            if nt == "entity":
                entities.append(d)
            elif nt == "dimension":
                dimensions.append(d)
            elif nt == "metric":
                metrics.append(d)

        yaml_dict = {
            "version": "1.0",
            "db_name": catalog._db_name,
            "entities": entities,
            "dimensions": dimensions,
            "metrics": metrics,
            "business": [],
        }
        stats = catalog.import_yaml(
            _yaml.dump(yaml_dict, allow_unicode=True, default_flow_style=False),
            save_to_db=True,
        )
        invalidate_semantic_cache()
        global _semantic_pipeline
        _semantic_pipeline = None

        return {
            "status": "ok",
            "applied": len(entities) + len(dimensions) + len(metrics),
            "catalog_stats": stats,
        }
    except Exception as e:
        logger.exception(f"[semantic/scan/apply] 失败: {e}")
        raise HTTPException(500, str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8765, log_level="info")
