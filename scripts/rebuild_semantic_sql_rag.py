from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from vanna_skill import load_config
from vanna_skill.doris_client import DorisClient
from vanna_skill.qwen_client import QwenClient
from vanna_skill.semantic.semantic_sql_rag import SemanticSQLRAGStore


def main() -> int:
    cfg = load_config()
    conn = dict(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg.get("password", ""),
    )
    sem = DorisClient(**conn, database="semantic_store")
    vec = DorisClient(**conn, database="vanna_store")
    qwen = QwenClient(
        api_key=cfg["qwen_api_key"],
        model=cfg.get("model", "qwen-plus"),
        embedding_model=cfg.get("embedding_model", "text-embedding-v3"),
    )
    store = SemanticSQLRAGStore(
        sem,
        vec,
        qwen,
        db_name=cfg.get("database", "retail_dw"),
    )
    result = store.rebuild_from_feedback_sources()
    print("Semantic SQL RAG rebuild:", result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
