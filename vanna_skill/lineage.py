"""
SQL 血缘分析器
- 解析 SQL（用 sqlparse）提取表级血缘关系
- 从 audit_log 历史 SQL 构建全量血缘图
- 输出 Plotly 网络图（用于 UI 展示）
"""
from __future__ import annotations

import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Optional, Set, Tuple

import pandas as pd
import sqlparse

if TYPE_CHECKING:
    from .doris_client import DorisClient

logger = logging.getLogger(__name__)


@dataclass
class LineageEdge:
    source_table: str      # 数据来源表
    target_table: str      # 数据目标表（INSERT INTO / CREATE TABLE AS）
    sql_preview: str = ""  # SQL 片段
    sql_type: str = ""     # SELECT / INSERT / CREATE
    freq: int = 1          # 出现次数


@dataclass
class LineageGraph:
    edges: List[LineageEdge] = field(default_factory=list)
    # table -> 依赖的上游表集合
    upstream: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))
    # table -> 被依赖的下游表集合
    downstream: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))

    def add_edge(self, src: str, tgt: str, sql: str = "", sql_type: str = ""):
        # 合并相同 edge 的 freq
        for e in self.edges:
            if e.source_table == src and e.target_table == tgt:
                e.freq += 1
                return
        self.edges.append(LineageEdge(src, tgt, sql[:100], sql_type))
        self.upstream[tgt].add(src)
        self.downstream[src].add(tgt)

    def get_upstream_tables(self, table: str, depth: int = 3) -> Set[str]:
        """BFS 向上追溯上游表（最多 depth 层）"""
        result, queue = set(), [table]
        for _ in range(depth):
            next_q = []
            for t in queue:
                for up in self.upstream.get(t, []):
                    if up not in result:
                        result.add(up)
                        next_q.append(up)
            queue = next_q
        return result

    def get_downstream_tables(self, table: str, depth: int = 3) -> Set[str]:
        """BFS 向下追溯下游表"""
        result, queue = set(), [table]
        for _ in range(depth):
            next_q = []
            for t in queue:
                for down in self.downstream.get(t, []):
                    if down not in result:
                        result.add(down)
                        next_q.append(down)
            queue = next_q
        return result

    def impact_analysis(self, table: str) -> dict:
        """影响分析：修改 table 会影响哪些下游"""
        downstream = self.get_downstream_tables(table)
        upstream = self.get_upstream_tables(table)
        return {
            "table": table,
            "upstream_count": len(upstream),
            "upstream_tables": sorted(upstream),
            "downstream_count": len(downstream),
            "downstream_tables": sorted(downstream),
        }

    def all_tables(self) -> Set[str]:
        tables = set()
        for e in self.edges:
            tables.add(e.source_table)
            tables.add(e.target_table)
        return tables

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame([{
            "source": e.source_table,
            "target": e.target_table,
            "type": e.sql_type,
            "freq": e.freq,
            "sql_preview": e.sql_preview,
        } for e in self.edges])


# ── SQL 解析工具函数 ───────────────────────────────────────────────────────────

def _extract_from_tables(sql: str) -> Set[str]:
    """从 SQL 中提取所有 FROM/JOIN 引用的表名"""
    tables = set()
    patterns = [
        r"\bFROM\s+`?(\w+(?:\.\w+)?)`?",
        r"\bJOIN\s+`?(\w+(?:\.\w+)?)`?",
    ]
    for pat in patterns:
        for m in re.finditer(pat, sql, re.IGNORECASE):
            t = m.group(1).lower().split(".")[-1]  # 去掉 db. 前缀
            if len(t) > 2:  # 过滤太短的
                tables.add(t)
    return tables


def _extract_target_table(sql: str) -> Tuple[Optional[str], str]:
    """
    提取写入目标表和 SQL 类型
    - INSERT INTO xxx → (xxx, INSERT)
    - CREATE TABLE xxx AS SELECT → (xxx, CREATE)
    - 纯 SELECT → (None, SELECT)
    """
    insert_m = re.search(
        r"\bINSERT\s+(?:OVERWRITE\s+)?(?:INTO\s+)?`?(\w+(?:\.\w+)?)`?",
        sql, re.IGNORECASE
    )
    if insert_m:
        t = insert_m.group(1).lower().split(".")[-1]
        return t, "INSERT"

    create_m = re.search(
        r"\bCREATE\s+(?:TABLE|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?`?(\w+(?:\.\w+)?)`?",
        sql, re.IGNORECASE
    )
    if create_m:
        t = create_m.group(1).lower().split(".")[-1]
        return t, "CREATE"

    return None, "SELECT"


def parse_sql_lineage(sql: str) -> List[Tuple[str, str, str]]:
    """
    解析单条 SQL，返回血缘关系列表：[(source_table, target_table, sql_type)]
    纯 SELECT 无写入则返回空列表
    """
    sql = sql.strip()
    target, sql_type = _extract_target_table(sql)
    if not target:
        return []

    sources = _extract_from_tables(sql)
    sources.discard(target)  # 不包含自引用

    return [(src, target, sql_type) for src in sources if src != target]


# ── 血缘管理器 ────────────────────────────────────────────────────────────────

class LineageManager:
    """
    从多来源构建和维护血缘图：
    1. audit_log 历史 SQL（大量 ETL 语句）
    2. 手动录入的 SQL
    3. Vanna 知识库中的 SQL
    """

    def __init__(self, biz_doris: "DorisClient" = None, vec_doris: "DorisClient" = None):  # type: ignore
        self._biz = biz_doris
        self._vec = vec_doris
        self.graph = LineageGraph()

    def build_from_audit_log(self, limit: int = 5000) -> int:
        """从 audit_log 构建血缘"""
        if not self._biz:
            logger.warning("[Lineage] 无 Doris 业务连接，跳过 audit_log 加载")
            return 0

        try:
            rows = self._biz.execute(f"""
                SELECT stmt
                FROM __internal_schema.audit_log
                WHERE stmt REGEXP '^\\s*(INSERT|CREATE)'
                  AND state = 'EOF'
                ORDER BY query_time DESC
                LIMIT {limit}
            """)
        except Exception as e:
            logger.warning(f"[Lineage] audit_log 查询失败: {e}")
            return 0

        count = 0
        for row in rows:
            sql = row.get("stmt", "")
            edges = parse_sql_lineage(sql)
            for src, tgt, stype in edges:
                self.graph.add_edge(src, tgt, sql[:100], stype)
                count += 1

        logger.info(f"[Lineage] 从 audit_log 构建 {count} 条血缘关系")
        return count

    def build_from_vanna_knowledge(self) -> int:
        """从 Vanna 知识库 SQL 补充血缘"""
        if not self._vec:
            return 0
        try:
            rows = self._vec.execute("""
                SELECT content AS sql_text
                FROM vanna_store.vanna_embeddings
                WHERE content_type = 'sql'
            """)
        except Exception as e:
            logger.warning(f"[Lineage] vanna 知识库查询失败: {e}")
            return 0

        count = 0
        for row in rows:
            edges = parse_sql_lineage(row.get("sql_text", ""))
            for src, tgt, stype in edges:
                self.graph.add_edge(src, tgt, "", stype)
                count += 1
        return count

    def add_sql(self, sql: str):
        """手动添加单条 SQL 到血缘图"""
        for src, tgt, stype in parse_sql_lineage(sql):
            self.graph.add_edge(src, tgt, sql[:100], stype)

    def get_lineage_df(self) -> pd.DataFrame:
        return self.graph.to_dataframe()

    def impact_analysis(self, table: str) -> dict:
        return self.graph.impact_analysis(table)

    # ─────────────────────────────────────────────────────────────────────────
    # Plotly 可视化
    # ─────────────────────────────────────────────────────────────────────────
    def to_plotly_figure(self, highlight_table: str = "") -> Optional[object]:
        """生成 Plotly 网络图"""
        try:
            import plotly.graph_objects as go
        except ImportError:
            return None

        tables = list(self.graph.all_tables())
        if not tables:
            return None

        # 简单环形布局
        import math
        n = len(tables)
        pos = {t: (math.cos(2 * math.pi * i / n),
                   math.sin(2 * math.pi * i / n))
               for i, t in enumerate(tables)}

        # 边
        edge_x, edge_y = [], []
        for e in self.graph.edges:
            if e.source_table in pos and e.target_table in pos:
                x0, y0 = pos[e.source_table]
                x1, y1 = pos[e.target_table]
                edge_x += [x0, x1, None]
                edge_y += [y0, y1, None]

        # 节点颜色
        def node_color(t):
            if t == highlight_table:
                return "#FF6B6B"
            if t in self.graph.upstream.get(highlight_table, set()):
                return "#FFA94D"
            if t in self.graph.downstream.get(highlight_table, set()):
                return "#74C0FC"
            return "#69DB7C"

        node_x = [pos[t][0] for t in tables]
        node_y = [pos[t][1] for t in tables]
        node_colors = [node_color(t) for t in tables]
        node_text = [
            f"{t}<br>上游:{len(self.graph.upstream.get(t,[]))}"
            f" 下游:{len(self.graph.downstream.get(t,[]))}"
            for t in tables
        ]

        fig = go.Figure(
            data=[
                go.Scatter(x=edge_x, y=edge_y, mode="lines",
                           line=dict(width=1, color="#aaa"),
                           hoverinfo="none"),
                go.Scatter(x=node_x, y=node_y, mode="markers+text",
                           marker=dict(size=20, color=node_colors,
                                       line=dict(width=2, color="#333")),
                           text=tables,
                           textposition="top center",
                           hovertext=node_text,
                           hoverinfo="text"),
            ],
            layout=go.Layout(
                title="表级血缘图",
                showlegend=False,
                hovermode="closest",
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                height=600,
                margin=dict(l=20, r=20, t=40, b=20),
                paper_bgcolor="#0E1117",
                plot_bgcolor="#0E1117",
                font=dict(color="#FAFAFA"),
            ),
        )
        return fig


