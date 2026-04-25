"""
元数据管理器
- 从 information_schema 自动同步表/列元数据
- 支持手动追加业务描述
- 生成 DDL 供 Vanna 训练
"""
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd

from .doris_client import DorisClient

logger = logging.getLogger(__name__)


@dataclass
class ColumnMeta:
    column_name: str
    data_type: str
    is_nullable: str
    column_comment: str
    ordinal_position: int


@dataclass
class TableMeta:
    table_schema: str
    table_name: str
    table_comment: str
    engine: str
    table_rows: int
    create_time: str
    columns: List[ColumnMeta]

    def to_ddl(self) -> str:
        """生成 CREATE TABLE DDL（供 Vanna 训练）"""
        lines = [f"-- {self.table_comment}",
                 f"CREATE TABLE `{self.table_name}` ("]
        for col in self.columns:
            comment_str = f" COMMENT '{col.column_comment}'" if col.column_comment else ""
            nullable = "" if col.is_nullable == "YES" else " NOT NULL"
            lines.append(f"  `{col.column_name}` {col.data_type}{nullable}{comment_str},")
        lines[-1] = lines[-1].rstrip(",")  # 移除最后逗号
        lines.append(f") COMMENT '{self.table_comment}';")
        return "\n".join(lines)

    def summary(self) -> str:
        """简要文字描述（供 Vanna doc 训练）"""
        col_desc = "、".join(
            f"{c.column_name}({c.column_comment or c.data_type})"
            for c in self.columns[:8]
        )
        suffix = f"，共{len(self.columns)}列" if len(self.columns) > 8 else ""
        return (
            f"表 {self.table_name}：{self.table_comment or '无描述'}。"
            f"主要字段：{col_desc}{suffix}。"
        )


class MetadataManager:
    """
    元数据管理入口
    """

    def __init__(self, doris: DorisClient, db_name: str):
        self._db = doris
        self._db_name = db_name
        self._cache: Dict[str, TableMeta] = {}   # table_name -> TableMeta

    # ─────────────────────────────────────────────────────────────────────────
    # 核心：从 information_schema 同步
    # ─────────────────────────────────────────────────────────────────────────

    def sync(self) -> List[TableMeta]:
        """从 information_schema 全量同步当前库的所有表元数据"""
        tables_df = self._db.query_df("""
            SELECT TABLE_SCHEMA, TABLE_NAME, TABLE_COMMENT,
                   ENGINE, TABLE_ROWS,
                   DATE_FORMAT(CREATE_TIME, '%%Y-%%m-%%d %%H:%%i') AS create_time
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s
              AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """, (self._db_name,))

        cols_df = self._db.query_df("""
            SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE,
                   IS_NULLABLE, COLUMN_COMMENT, ORDINAL_POSITION
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s
            ORDER BY TABLE_NAME, ORDINAL_POSITION
        """, (self._db_name,))

        result = []
        for _, row in tables_df.iterrows():
            tname = row["TABLE_NAME"]
            tcols = cols_df[cols_df["TABLE_NAME"] == tname]
            cols = [
                ColumnMeta(
                    column_name=c["COLUMN_NAME"],
                    data_type=c["DATA_TYPE"],
                    is_nullable=c["IS_NULLABLE"],
                    column_comment=c.get("COLUMN_COMMENT", ""),
                    ordinal_position=c["ORDINAL_POSITION"],
                )
                for _, c in tcols.iterrows()
            ]
            meta = TableMeta(
                table_schema=row["TABLE_SCHEMA"],
                table_name=tname,
                table_comment=row.get("TABLE_COMMENT", ""),
                engine=row.get("ENGINE", ""),
                table_rows=row.get("TABLE_ROWS", 0) or 0,
                create_time=str(row.get("create_time", "")),
                columns=cols,
            )
            self._cache[tname] = meta
            result.append(meta)

        logger.info(f"[Metadata] 同步完成，共 {len(result)} 张表")
        return result

    def get_table(self, table_name: str) -> Optional[TableMeta]:
        if not self._cache:
            self.sync()
        return self._cache.get(table_name)

    def all_tables(self) -> List[TableMeta]:
        if not self._cache:
            self.sync()
        return list(self._cache.values())

    def describe_table(self, table_name: str) -> str:
        """供 LangChain Tool 调用：返回表的文字描述"""
        meta = self.get_table(table_name)
        if not meta:
            return f"未找到表 {table_name}，请检查表名是否正确"
        lines = [
            f"**{meta.table_name}** — {meta.table_comment}",
            f"引擎: {meta.engine}  预估行数: {meta.table_rows:,}",
            "",
            "| 字段名 | 类型 | 说明 |",
            "|---|---|---|",
        ]
        for col in meta.columns:
            lines.append(
                f"| {col.column_name} | {col.data_type} | {col.column_comment} |"
            )
        return "\n".join(lines)

    def to_dataframe(self) -> pd.DataFrame:
        """所有表概览，返回 DataFrame"""
        metas = self.all_tables()
        return pd.DataFrame([{
            "table_name": m.table_name,
            "comment": m.table_comment,
            "engine": m.engine,
            "rows": m.table_rows,
            "col_count": len(m.columns),
            "create_time": m.create_time,
        } for m in metas])

    def columns_dataframe(self) -> pd.DataFrame:
        """所有列信息，返回 DataFrame"""
        rows = []
        for m in self.all_tables():
            for c in m.columns:
                rows.append({
                    "table": m.table_name,
                    "table_comment": m.table_comment,
                    "column": c.column_name,
                    "type": c.data_type,
                    "nullable": c.is_nullable,
                    "comment": c.column_comment,
                })
        return pd.DataFrame(rows)

    # ─────────────────────────────────────────────────────────────────────────
    # 生成 DDL / doc 供 Vanna 训练
    # ─────────────────────────────────────────────────────────────────────────

    def generate_all_ddl(self) -> List[str]:
        """生成所有表的 DDL 字符串列表"""
        return [m.to_ddl() for m in self.all_tables()]

    def generate_all_summaries(self) -> List[str]:
        """生成所有表的业务摘要字符串列表"""
        return [m.summary() for m in self.all_tables()]

    # ─────────────────────────────────────────────────────────────────────────
    # table_properties（Doris 特有）
    # ─────────────────────────────────────────────────────────────────────────

    def get_table_properties(self) -> pd.DataFrame:
        """查询 information_schema.table_properties（Doris 特有）"""
        try:
            return self._db.query_df("""
                SELECT TABLE_NAME, PROPERTY_NAME, PROPERTY_VALUE
                FROM information_schema.table_properties
                WHERE TABLE_SCHEMA = %s
                ORDER BY TABLE_NAME, PROPERTY_NAME
            """, (self._db_name,))
        except Exception as e:
            logger.warning(f"[Metadata] table_properties 查询失败: {e}")
            return pd.DataFrame()
