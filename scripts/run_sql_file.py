#!/usr/bin/env python3
"""执行 SQL 文件。

示例：
  ./venv/bin/python scripts/run_sql_file.py sql/mock_rca_data_2026.sql
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pymysql


ROOT = Path(__file__).resolve().parents[1]


def split_sql(sql: str) -> list[str]:
    statements: list[str] = []
    buf: list[str] = []
    in_single = False
    in_double = False
    escape = False
    for ch in sql:
        buf.append(ch)
        if escape:
            escape = False
            continue
        if ch == "\\":
            escape = True
            continue
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == ";" and not in_single and not in_double:
            statement = "".join(buf).strip().rstrip(";").strip()
            if statement:
                statements.append(statement)
            buf = []
    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements


def main() -> int:
    if len(sys.argv) != 2:
        print("Usage: run_sql_file.py <sql-file>", file=sys.stderr)
        return 2

    sql_path = (ROOT / sys.argv[1]).resolve()
    if not sql_path.exists():
        print(f"SQL file not found: {sql_path}", file=sys.stderr)
        return 2

    config = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))
    conn = pymysql.connect(
        host=config["host"],
        port=int(config["port"]),
        user=config["user"],
        password=config.get("password", ""),
        database=config.get("database", ""),
        charset="utf8mb4",
        autocommit=True,
        connect_timeout=10,
    )
    sql_text = sql_path.read_text(encoding="utf-8")
    statements = split_sql(sql_text)
    with conn.cursor() as cur:
        for idx, statement in enumerate(statements, start=1):
            cur.execute(statement)
            head = statement.splitlines()[0][:90]
            print(f"[{idx:03d}/{len(statements):03d}] OK {head}")
    print(f"Done. Executed {len(statements)} statements from {sql_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
