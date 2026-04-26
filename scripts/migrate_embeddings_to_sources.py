"""
将历史 vanna_embeddings 数据迁移到拆分后的源数据表：
- vanna_sql
- vanna_doc
- vanna_metadata
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from vanna_skill import DorisClient, load_config


def _guess_table_name(row: dict) -> str:
    table_names = (row.get("table_names") or "").strip()
    if table_names:
        return table_names.split(",")[0].strip().lower()

    content = row.get("content") or ""
    ddl_match = re.search(r"\bCREATE\s+TABLE\s+`?(\w+(?:\.\w+)?)`?", content, re.IGNORECASE)
    if ddl_match:
        return ddl_match.group(1).split(".")[-1].lower()

    doc_match = re.search(r"表\s+([a-zA-Z0-9_]+)\s*：", content)
    if doc_match:
        return doc_match.group(1).lower()
    return ""


def main():
    cfg = load_config()
    vec = DorisClient(
        host=cfg["host"],
        port=cfg["port"],
        user=cfg["user"],
        password=cfg.get("password", ""),
        database="vanna_store",
    )

    rows = vec.execute(
        """
        SELECT id, content_type, question, content, source, db_name, table_names,
               quality_score, use_count, created_at, updated_at
        FROM vanna_store.vanna_embeddings
        ORDER BY created_at ASC
        """
    )

    sql_rows = [row for row in rows if row["content_type"] == "sql"]
    ddl_rows = [row for row in rows if row["content_type"] == "ddl"]
    doc_rows = [row for row in rows if row["content_type"] == "doc"]

    schema_doc_map = {}
    for row in doc_rows:
        if (row.get("source") or "") != "schema":
            continue
        table_name = _guess_table_name(row)
        if table_name and table_name not in schema_doc_map:
            schema_doc_map[table_name] = row.get("content") or ""

    sql_added = 0
    for row in sql_rows:
        exists = vec.execute(
            "SELECT 1 FROM vanna_store.vanna_sql WHERE id = %s LIMIT 1",
            (row["id"],),
        )
        if exists:
            continue
        vec.execute_write(
            """
            INSERT INTO vanna_store.vanna_sql
                (id, question, sql_text, source, db_name, table_names,
                 quality_score, use_count, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                row["id"],
                row.get("question") or "",
                row.get("content") or "",
                row.get("source") or "",
                row.get("db_name") or "",
                row.get("table_names") or "",
                row.get("quality_score") or 1.0,
                row.get("use_count") or 0,
                row.get("created_at"),
                row.get("updated_at"),
            ),
        )
        sql_added += 1

    doc_added = 0
    for row in doc_rows:
        exists = vec.execute(
            "SELECT 1 FROM vanna_store.vanna_doc WHERE id = %s LIMIT 1",
            (row["id"],),
        )
        if exists:
            continue
        table_name = _guess_table_name(row)
        title = table_name or (row.get("source") or "doc")
        vec.execute_write(
            """
            INSERT INTO vanna_store.vanna_doc
                (id, title, content, source, db_name, table_names,
                 quality_score, use_count, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                row["id"],
                title,
                row.get("content") or "",
                row.get("source") or "",
                row.get("db_name") or "",
                row.get("table_names") or table_name,
                row.get("quality_score") or 1.0,
                row.get("use_count") or 0,
                row.get("created_at"),
                row.get("updated_at"),
            ),
        )
        doc_added += 1

    metadata_added = 0
    for row in ddl_rows:
        exists = vec.execute(
            "SELECT 1 FROM vanna_store.vanna_metadata WHERE id = %s LIMIT 1",
            (row["id"],),
        )
        if exists:
            continue
        table_name = _guess_table_name(row) or f"unknown_{row['id']}"
        vec.execute_write(
            """
            INSERT INTO vanna_store.vanna_metadata
                (id, table_name, db_name, table_comment, engine, table_rows,
                 ddl_text, summary_text, source, quality_score, use_count,
                 created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                row["id"],
                table_name,
                row.get("db_name") or "",
                "",
                "",
                0,
                row.get("content") or "",
                schema_doc_map.get(table_name, ""),
                row.get("source") or "",
                row.get("quality_score") or 1.0,
                row.get("use_count") or 0,
                row.get("created_at"),
                row.get("updated_at"),
            ),
        )
        metadata_added += 1

    print({
        "sql_added": sql_added,
        "doc_added": doc_added,
        "metadata_added": metadata_added,
        "total_embeddings": len(rows),
    })


if __name__ == "__main__":
    main()
