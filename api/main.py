"""
Vanna Skill REST API  +  SSE 实时推送
运行: uvicorn api.main:app --host 0.0.0.0 --port 8765 --reload
"""
import json
import logging
import queue as queue_module
import re
import sys
import threading
from pathlib import Path
from typing import Optional

import pandas as pd
sys.path.insert(0, str(Path(__file__).parent.parent))

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from vanna_skill import DorisVanna, tracer, load_config, save_config, AskLCPipeline, DorisClient, PromptStore, LineageManager, invalidate_lineage_cache
from vanna_skill.pipelines.semantic_pipeline import SemanticPipeline
from vanna_skill.semantic.catalog import invalidate_semantic_cache
from vanna_skill.semantic.schema_scanner import SchemaScanner

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(name)s  %(message)s")
logger = logging.getLogger(__name__)

_READ_ONLY_SQL_PATTERN = re.compile(r"^\s*(WITH\b|SELECT\b)", re.IGNORECASE)

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


@app.get("/health")
def health():
    try:
        vn = get_vanna()
        ok = vn._biz.test()
        stats = vn.gemini_stats
        return {"status": "healthy" if ok else "degraded",
                "doris": "ok" if ok else "fail",
                "qwen_stats": stats,
                "gemini_stats": stats,
                "tracer_stats": tracer.stats()}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}


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


@app.post("/execute")
def execute_sql(body: dict):
    """执行 SQL 并返回结果"""
    sql = body.get("sql", "")
    if not _READ_ONLY_SQL_PATTERN.match(sql or ""):
        raise HTTPException(400, "只允许只读查询（SELECT / WITH ... SELECT）")
    try:
        logger.info("[execute] 执行 SQL:\n%s", sql)
        df = get_vanna().run_sql(sql)
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
    if req.is_correct:
        vn.add_question_sql(req.question, req.sql, source="feedback")
        return {"status": "ok", "action": "added"}
    elif req.corrected_sql:
        vn.add_question_sql(req.question, req.corrected_sql,
                            source="feedback_corrected")
        return {"status": "ok", "action": "corrected"}
    return {"status": "ok", "action": "negative_recorded"}


# ── 调用链路日志 ──────────────────────────────────────────────────────────────

@app.get("/traces")
def get_traces(n: int = 100):
    vn = get_vanna()
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
    global CONFIG, _vanna, _ask_lc_pipeline, _semantic_pipeline, _prompt_store
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
    })
    if qwen_api_key and "..." not in qwen_api_key:
        updated["qwen_api_key"] = qwen_api_key
    CONFIG = save_config(updated)
    _vanna = None  # 重置连接，下次调用重新初始化
    _ask_lc_pipeline = None
    _semantic_pipeline = None
    _prompt_store = None
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
