#!/usr/bin/env python3
"""
从 Doris 业务表采集低基数维度枚举，写入 cube_store.cube_dimension_values。

默认只采集 cube_dimensions 中 sql_expr 为简单列名的 string/number/boolean 维度。
用法:
  ./venv/bin/python scripts/collect_cube_dimension_values.py --max-values 200
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from vanna_skill import DorisClient, load_config


_SIMPLE_COLUMN_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _stable_id(*parts: Any) -> int:
    raw = "\n".join(str(part or "") for part in parts)
    return int(hashlib.md5(raw.encode("utf-8")).hexdigest()[:15], 16)


def _literal(value: str) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-values", type=int, default=200, help="每个维度最多采集多少个不同值")
    parser.add_argument("--max-cardinality", type=int, default=500, help="超过该去重值数量则跳过")
    parser.add_argument("--cube-db", default="cube_store")
    args = parser.parse_args()

    config = load_config()
    client = DorisClient(
        host=config["host"],
        port=config["port"],
        user=config["user"],
        password=config.get("password", ""),
        database=args.cube_db,
    )

    dims = client.execute(
        """
        SELECT d.cube_name, d.dimension_name, d.sql_expr, d.dimension_type, m.sql_table
        FROM cube_store.cube_dimensions d
        JOIN cube_store.cube_models m ON d.cube_name = m.cube_name
        WHERE d.visible = 1
          AND m.visible = 1
          AND d.dimension_type IN ('string', 'number', 'boolean')
        ORDER BY d.cube_name, d.dimension_name
        """
    )

    inserted = 0
    skipped = 0
    for dim in dims:
        cube_name = dim["cube_name"]
        dimension_name = dim["dimension_name"]
        column = (dim.get("sql_expr") or "").strip()
        table = (dim.get("sql_table") or "").strip()
        if not table or not _SIMPLE_COLUMN_RE.match(column):
            skipped += 1
            print(f"SKIP {cube_name}.{dimension_name}: sql_expr 非简单列名: {column}")
            continue

        rows = client.execute(
            f"""
            SELECT CAST({column} AS STRING) AS value_code, COUNT(*) AS usage_count
            FROM {table}
            WHERE {column} IS NOT NULL
            GROUP BY {column}
            ORDER BY usage_count DESC
            LIMIT {int(args.max_cardinality) + 1}
            """
        )
        if len(rows) > args.max_cardinality:
            skipped += 1
            print(f"SKIP {cube_name}.{dimension_name}: 基数过高 rows>{args.max_cardinality}")
            continue

        for row in rows[: args.max_values]:
            value_code = str(row.get("value_code") or "").strip()
            if not value_code:
                continue
            value_id = _stable_id(cube_name, dimension_name, value_code)
            usage_count = int(row.get("usage_count", 0) or 0)
            client.execute_write(
                f"""
                INSERT INTO cube_store.cube_dimension_values
                  (value_id, cube_name, dimension_name, value_code, value_label,
                   aliases_json, source, source_table, source_column, usage_count, visible, version)
                VALUES
                  ({value_id}, {_literal(cube_name)}, {_literal(dimension_name)},
                   {_literal(value_code)}, {_literal(value_code)}, {_literal(json.dumps([], ensure_ascii=False))},
                   'scan', {_literal(table)}, {_literal(column)}, {usage_count}, 1, 1)
                """
            )
            inserted += 1
        print(f"OK {cube_name}.{dimension_name}: collected={min(len(rows), args.max_values)}")

    print(json.dumps({"inserted": inserted, "skipped": skipped}, ensure_ascii=False))


if __name__ == "__main__":
    main()

