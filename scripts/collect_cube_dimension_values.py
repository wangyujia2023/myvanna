#!/usr/bin/env python3
"""
从 Doris 业务表采集低基数维度枚举，写入 cube_store.cube_dimension_values。

默认只采集 cube_dimensions 中 sql_expr 为简单列名的 string/number/boolean 维度。
默认跳过 orders/users/refunds 等明细事实 Cube，避免采集订单号、用户号等高噪音枚举。
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
_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _stable_id(*parts: Any) -> int:
    raw = "\n".join(str(part or "") for part in parts)
    return int(hashlib.md5(raw.encode("utf-8")).hexdigest()[:15], 16)


def _literal(value: str) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


def _safe_identifier(value: str, label: str) -> str:
    text = str(value or "").strip()
    if not _IDENTIFIER_RE.match(text):
        raise ValueError(f"{label} 非法: {value}")
    return text


def _primary_key_columns(rows: list[dict[str, Any]]) -> dict[str, str]:
    result: dict[str, str] = {}
    for row in rows:
        cube_name = str(row.get("cube_name") or "")
        column = str(row.get("sql_expr") or "").strip()
        if cube_name and row.get("primary_key_flag") and _SIMPLE_COLUMN_RE.match(column):
            result.setdefault(cube_name, column)
    return result


def _label_columns(rows: list[dict[str, Any]]) -> dict[str, str]:
    result: dict[str, str] = {}
    for row in rows:
        cube_name = str(row.get("cube_name") or "")
        dimension_name = str(row.get("dimension_name") or "")
        column = str(row.get("sql_expr") or "").strip()
        if not cube_name or not _SIMPLE_COLUMN_RE.match(column):
            continue
        if dimension_name.endswith("_name") or dimension_name in {"name", "title"}:
            result.setdefault(cube_name, column)
    return result


def _enum_collect_columns(
    cube_name: str,
    dimension_name: str,
    dimension_column: str,
    key_columns: dict[str, str],
    label_columns: dict[str, str],
) -> tuple[str, str]:
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


def _is_collect_option_visible(cube_name: str, dimension_name: str) -> bool:
    # store_id is collected through stores.store_name as value_code.
    return not (cube_name == "stores" and dimension_name == "store_id")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-values", type=int, default=200, help="每个维度最多采集多少个不同值")
    parser.add_argument("--max-cardinality", type=int, default=500, help="超过该去重值数量则跳过")
    parser.add_argument("--exclude-cubes", default="orders,users,refunds", help="逗号分隔，默认不从这些 Cube 自动采集枚举")
    parser.add_argument("--cube-db", default="cube_store")
    args = parser.parse_args()
    exclude_cubes = {item.strip() for item in args.exclude_cubes.split(",") if item.strip()}
    cube_db = _safe_identifier(args.cube_db, "--cube-db")

    config = load_config()
    client = DorisClient(
        host=config["host"],
        port=config["port"],
        user=config["user"],
        password=config.get("password", ""),
        database=cube_db,
    )

    dims = client.execute(
        f"""
        SELECT d.cube_name, d.dimension_name, d.sql_expr, d.dimension_type,
               d.primary_key_flag, m.sql_table
        FROM {cube_db}.cube_dimensions d
        JOIN {cube_db}.cube_models m ON d.cube_name = m.cube_name
        WHERE d.visible = 1
          AND m.visible = 1
          AND d.dimension_type IN ('string', 'number', 'boolean')
        ORDER BY d.cube_name, d.dimension_name
        """
    )

    inserted = 0
    skipped = 0
    key_columns = _primary_key_columns(dims)
    label_columns = _label_columns(dims)
    for dim in dims:
        cube_name = dim["cube_name"]
        dimension_name = dim["dimension_name"]
        column = (dim.get("sql_expr") or "").strip()
        table = (dim.get("sql_table") or "").strip()
        if not _is_collect_option_visible(cube_name, dimension_name):
            skipped += 1
            print(f"SKIP {cube_name}.{dimension_name}: 由 stores.store_name 统一采集门店 code/label")
            continue
        if cube_name in exclude_cubes:
            skipped += 1
            print(f"SKIP {cube_name}.{dimension_name}: 明细/事实 Cube 默认不自动采集枚举")
            continue
        if not table or not _SIMPLE_COLUMN_RE.match(column):
            skipped += 1
            print(f"SKIP {cube_name}.{dimension_name}: sql_expr 非简单列名: {column}")
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
            LIMIT {int(args.max_cardinality) + 1}
            """
        )
        if len(rows) > args.max_cardinality:
            skipped += 1
            print(f"SKIP {cube_name}.{dimension_name}: 基数过高 rows>{args.max_cardinality}")
            continue

        for row in rows[: args.max_values]:
            value_code = str(row.get("value_code") or "").strip()
            value_label = str(row.get("value_label") or value_code).strip()
            if not value_code:
                continue
            value_id = _stable_id(cube_name, dimension_name, value_code)
            usage_count = int(row.get("usage_count", 0) or 0)
            client.execute_write(
                f"""
                INSERT INTO {cube_db}.cube_dimension_values
                  (value_id, cube_name, dimension_name, value_code, value_label,
                   aliases_json, source, source_table, source_column, usage_count, visible, version)
                VALUES
                  ({value_id}, {_literal(cube_name)}, {_literal(dimension_name)},
                   {_literal(value_code)}, {_literal(value_label)}, {_literal(json.dumps([], ensure_ascii=False))},
                   'scan', {_literal(table)}, {_literal(label_column)}, {usage_count}, 1, 1)
                """
            )
            inserted += 1
        print(f"OK {cube_name}.{dimension_name}: collected={min(len(rows), args.max_values)}")

    print(json.dumps({"inserted": inserted, "skipped": skipped}, ensure_ascii=False))


if __name__ == "__main__":
    main()
