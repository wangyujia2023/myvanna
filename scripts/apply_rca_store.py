#!/usr/bin/env python3
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pymysql

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from vanna_skill.config_store import load_config


def main() -> None:
    cfg = load_config()
    sql_path = Path("sql/rca_store.sql")
    raw_sql = sql_path.read_text(encoding="utf-8")
    statements = [stmt.strip() for stmt in re.split(r";\s*(?:\n|$)", raw_sql) if stmt.strip()]
    conn = pymysql.connect(
        host=cfg["host"],
        port=int(cfg["port"]),
        user=cfg["user"],
        password=cfg.get("password", ""),
        charset="utf8mb4",
        autocommit=True,
        connect_timeout=10,
    )
    executed = 0
    failed = []
    try:
        with conn.cursor() as cur:
            for index, statement in enumerate(statements, 1):
                try:
                    cur.execute(statement)
                    executed += 1
                except Exception as exc:
                    failed.append({
                        "index": index,
                        "sql": statement[:200],
                        "error": str(exc),
                    })
    finally:
        conn.close()
    print(json.dumps({"executed": executed, "failed": failed}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
