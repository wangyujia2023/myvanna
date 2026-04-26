"""
Vanna Skill REST API  +  SSE 实时推送
运行: uvicorn api.main:app --host 0.0.0.0 --port 8765 --reload
"""
import json
import logging
import queue as queue_module
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
    if not sql.strip().upper().startswith("SELECT"):
        raise HTTPException(400, "只允许 SELECT 查询")
    try:
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
    global CONFIG, _vanna, _ask_lc_pipeline, _prompt_store
    qwen_api_key = req.qwen_api_key.strip()
    updated = CONFIG.copy()
    updated.update({
        "host": req.host, "port": req.port,
        "user": req.user, "password": req.password,
        "database": req.database,
        "model": req.model,
        "n_results": req.n_results,
    })
    if qwen_api_key and "..." not in qwen_api_key:
        updated["qwen_api_key"] = qwen_api_key
    CONFIG = save_config(updated)
    _vanna = None  # 重置连接，下次调用重新初始化
    _ask_lc_pipeline = None
    _prompt_store = None
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8765, log_level="info")
