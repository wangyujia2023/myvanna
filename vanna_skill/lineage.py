"""
SQL 血缘分析器
- 解析 SQL（用 sqlparse）提取表级血缘关系
- 从 audit_log 历史 SQL 构建全量血缘图
- 输出 Plotly 网络图（用于 UI 展示）
"""
from __future__ import annotations

import hashlib
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
    relation_type: str = "table_lineage"  # table_lineage / query_join / query_access
    edge_source: str = ""  # audit_log / vanna_sql
    freq: int = 1          # 出现次数


@dataclass
class LineageGraph:
    edges: List[LineageEdge] = field(default_factory=list)
    # table -> 依赖的上游表集合
    upstream: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))
    # table -> 被依赖的下游表集合
    downstream: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))

    def add_edge(
        self,
        src: str,
        tgt: str,
        sql: str = "",
        sql_type: str = "",
        relation_type: str = "table_lineage",
        edge_source: str = "",
    ):
        # 合并相同 edge 的 freq
        for e in self.edges:
            if (
                e.source_table == src
                and e.target_table == tgt
                and e.sql_type == sql_type
                and e.relation_type == relation_type
            ):
                e.freq += 1
                return
        self.edges.append(LineageEdge(src, tgt, sql[:100], sql_type, relation_type, edge_source))
        if src != tgt:
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

    def load_edge(
        self,
        src: str,
        tgt: str,
        sql: str = "",
        sql_type: str = "",
        relation_type: str = "table_lineage",
        edge_source: str = "",
        freq: int = 1,
    ):
        """从 DB 恢复边时使用：直接写入 freq，不做去重（DB 已保证唯一）。"""
        edge = LineageEdge(src, tgt, sql[:100], sql_type, relation_type, edge_source, freq)
        self.edges.append(edge)
        if src != tgt:
            self.upstream[tgt].add(src)
            self.downstream[src].add(tgt)

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

def _extract_cte_names(sql: str) -> Set[str]:
    """提取 WITH 子句中定义的所有 CTE 名称，避免将其误识别为真实表名。"""
    names: Set[str] = set()
    # 匹配 WITH name AS ( 或 , name AS (
    for m in re.finditer(r'(?:WITH|,)\s*(\w+)\s+AS\s*\(', sql, re.IGNORECASE):
        names.add(m.group(1).lower())
    return names


def _extract_from_tables(sql: str, exclude: Set[str] | None = None) -> Set[str]:
    """从 SQL 中提取所有 FROM/JOIN 引用的真实表名。

    Args:
        exclude: 需要排除的名称集合（通常是 CTE 名）
    """
    _exclude = exclude or set()
    tables = set()
    patterns = [
        r"\bFROM\s+`?(\w+(?:\.\w+)?)`?",
        r"\bJOIN\s+`?(\w+(?:\.\w+)?)`?",
    ]
    for pat in patterns:
        for m in re.finditer(pat, sql, re.IGNORECASE):
            t = m.group(1).lower().split(".")[-1]  # 去掉 db. 前缀
            if len(t) > 2 and t not in _exclude:   # 过滤太短的 & CTE 名
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

    cte_names = _extract_cte_names(sql)           # 先提取 CTE 名
    sources = _extract_from_tables(sql, exclude=cte_names)  # 排除 CTE
    sources.discard(target)

    return [(src, target, sql_type) for src in sources if src != target]


def parse_sql_relationships(sql: str) -> List[dict]:
    """
    统一解析 SQL 关系：
    - INSERT/CREATE ... SELECT ... FROM 真实表 → table_lineage（高可信度）
    - SELECT ... FROM A JOIN B ...             → query_join（低可信度，辅助）

    不产生 query_access 自边（单表 SELECT 没有血缘价值）。
    CTE 名称会被自动过滤，不会产生假边。
    """
    lineage_edges = parse_sql_lineage(sql)
    if lineage_edges:
        return [
            {
                "source_table": src,
                "target_table": tgt,
                "sql_type": sql_type,
                "relation_type": "table_lineage",
            }
            for src, tgt, sql_type in lineage_edges
        ]

    sql_text = (sql or "").strip()
    if not sql_text or not re.match(r"^\s*select\b", sql_text, re.IGNORECASE):
        return []

    cte_names = _extract_cte_names(sql_text)
    tables = sorted(_extract_from_tables(sql_text, exclude=cte_names))

    # 少于 2 张真实表 → 无 join 血缘，不产生任何边
    if len(tables) < 2:
        return []

    result = []
    for i in range(len(tables)):
        for j in range(i + 1, len(tables)):
            result.append({
                "source_table": tables[i],
                "target_table": tables[j],
                "sql_type": "SELECT",
                "relation_type": "query_join",
            })
    return result


# ── 血缘管理器 ────────────────────────────────────────────────────────────────

class LineageManager:
    """
    从多来源构建和维护血缘图：
    1. audit_log 历史 SQL（大量 ETL 语句）
    2. 手动录入的 SQL
    3. Vanna 知识库中的 SQL
    """

    def __init__(
        self,
        biz_doris: "DorisClient" = None,  # type: ignore
        vec_doris: "DorisClient" = None,   # type: ignore
        cfg: dict | None = None,
    ):
        self._biz = biz_doris
        self._vec = vec_doris
        self._cfg: dict = cfg or {}
        self.graph = LineageGraph()

    def _audit_log_query(self, limit: int) -> List[dict]:
        """执行 audit_log 查询，返回原始行列表（失败返回空列表）。

        过滤策略（与手动诊断查询保持一致）：
        - 只取 INSERT / CREATE 类型语句
        - 排除 DBeaver 等客户端工具产生的元数据查询
        - 排除 vanna_store 内部写入（不是业务血缘）
        - 排除 __internal_schema 系统内部统计写入
        - 不过滤 state 字段（Doris DML state 值因版本而异，不稳定）
        - ORDER BY time DESC（时间戳），而非 query_time（执行耗时 ms）
        """
        if not self._biz:
            return []
        try:
            return self._biz.execute(f"""
                SELECT stmt
                FROM __internal_schema.audit_log
                WHERE (
                    LOWER(stmt) LIKE '%insert into%'
                    OR LOWER(stmt) LIKE '%insert overwrite%'
                    OR LOWER(stmt) LIKE '%create table%'
                    OR LOWER(stmt) LIKE '%create view%'
                    OR (LOWER(stmt) LIKE '%select%' AND LOWER(stmt) LIKE '% from %')
                )
                AND stmt NOT LIKE '/* ApplicationName=DBeaver%'
                AND LOWER(stmt) NOT LIKE '%vanna_store%'
                AND LOWER(stmt) NOT LIKE '%__internal_schema%'
                ORDER BY `time` DESC
                LIMIT {limit}
            """)
        except Exception as e:
            logger.warning(f"[Lineage] audit_log 查询失败: {e}")
            return []

    def build_from_audit_log(self, limit: int = 5000) -> int:
        """从 audit_log 构建血缘

        返回解析出的 edge 数量。
        只有 INSERT INTO ... SELECT ... FROM ... 和 CREATE TABLE ... AS SELECT ...
        类型的 SQL 才能产生表级血缘，纯 INSERT VALUES 不会产生 edge。

        若需同时知道「取回行数」，调用 build_from_audit_log_verbose()。
        """
        _, count = self.build_from_audit_log_verbose(limit)
        return count

    def build_from_audit_log_verbose(self, limit: int = 5000):
        """
        返回 (fetched_rows, edge_count) 元组，供 UI 诊断展示。
        """
        if not self._biz:
            logger.warning("[Lineage] 无 Doris 业务连接，跳过 audit_log 加载")
            return 0, 0

        rows = self._audit_log_query(limit)
        fetched = len(rows)
        count = 0
        for row in rows:
            sql = row.get("stmt", "")
            edges = parse_sql_relationships(sql)
            for edge in edges:
                # query_join 边（SELECT 多表关联）噪音较多，默认不纳入
                # 如需开启，在 config 里设置 include_query_joins=true
                if (
                    edge["relation_type"] == "query_join"
                    and not self._cfg.get("include_query_joins", False)
                ):
                    continue
                self.graph.add_edge(
                    edge["source_table"],
                    edge["target_table"],
                    sql[:100],
                    edge["sql_type"],
                    edge["relation_type"],
                    "audit_log",
                )
                count += 1

        logger.info(
            f"[Lineage] audit_log 取回 {fetched} 条，解析出 {count} 条 edge"
        )
        return fetched, count

    def diagnose_audit_log(self, limit: int = 200) -> dict:
        """
        诊断模式：分析 audit_log 中 INSERT/CREATE SQL 的结构，
        帮助判断为什么 build_from_audit_log 返回 0。

        返回：
        {
          "fetched":       int,   # 取回的 SQL 总行数
          "has_from":      int,   # 包含 FROM 子句的行数（能产生血缘）
          "values_only":   int,   # 纯 VALUES 插入（不能产生血缘）
          "parsed_edges":  int,   # 实际解析出 edge 数
          "samples_with_from":  [str, ...],  # 最多5条有 FROM 的 SQL 预览
          "samples_no_from":    [str, ...],  # 最多5条无 FROM 的 SQL 预览
        }
        """
        rows = self._audit_log_query(limit)
        fetched = len(rows)
        has_from, values_only, parsed_edges = 0, 0, 0
        samples_with_from: List[str] = []
        samples_no_from: List[str] = []

        for row in rows:
            sql = (row.get("stmt") or "").strip()
            sql_lower = sql.lower()
            if re.search(r'\bfrom\b', sql_lower):
                has_from += 1
                edges = parse_sql_relationships(sql)
                parsed_edges += len(edges)
                if len(samples_with_from) < 5:
                    samples_with_from.append(sql[:300])
            else:
                values_only += 1
                if len(samples_no_from) < 5:
                    samples_no_from.append(sql[:300])

        return {
            "fetched": fetched,
            "has_from": has_from,
            "values_only": values_only,
            "parsed_edges": parsed_edges,
            "samples_with_from": samples_with_from,
            "samples_no_from": samples_no_from,
        }

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
            sql_text = row.get("sql_text", "")
            edges = parse_sql_relationships(sql_text)
            for edge in edges:
                self.graph.add_edge(
                    edge["source_table"],
                    edge["target_table"],
                    sql_text[:100],
                    edge["sql_type"],
                    edge["relation_type"],
                    "vanna_sql",
                )
                count += 1
        return count

    def add_sql(self, sql: str):
        """手动添加单条 SQL 到血缘图"""
        for edge in parse_sql_relationships(sql):
            self.graph.add_edge(
                edge["source_table"],
                edge["target_table"],
                sql[:100],
                edge["sql_type"],
                edge["relation_type"],
                "manual",
            )

    def get_lineage_df(self) -> pd.DataFrame:
        return self.graph.to_dataframe()

    def impact_analysis(self, table: str) -> dict:
        return self.graph.impact_analysis(table)

    def lineage_table_count(self) -> int:
        if not self._vec:
            return 0
        try:
            rows = self._vec.execute(
                "SELECT COUNT(*) AS cnt FROM vanna_store.vanna_lineage"
            )
            return int(rows[0]["cnt"]) if rows else 0
        except Exception as e:
            logger.warning(f"[Lineage] 统计血缘表失败: {e}")
            return 0

    def rebuild_lineage_table(self) -> dict:
        if not self._vec:
            raise ValueError("缺少向量库连接，无法重建血缘表")

        self.graph = LineageGraph()
        audit_fetched, audit_edges = self.build_from_audit_log_verbose()
        knowledge_edges = self.build_from_vanna_knowledge()
        diagnosis = self.diagnose_audit_log(limit=200)
        rows = self.graph.to_dataframe()

        try:
            self._vec.execute_write("TRUNCATE TABLE vanna_store.vanna_lineage")
        except Exception as e:
            logger.warning(f"[Lineage] 清理血缘表失败: {e}")

        inserted = self._persist_graph_to_table()
        logger.info(
            "[Lineage] 重建完成 fetched=%s audit_edges=%s knowledge_edges=%s inserted=%s",
            audit_fetched, audit_edges, knowledge_edges, inserted,
        )
        return {
            "audit_fetched": audit_fetched,
            "audit_edges": audit_edges,
            "knowledge_edges": knowledge_edges,
            "total_edges": len(rows),
            "inserted": inserted,
            "diagnosis": diagnosis,
        }

    def _persist_graph_to_table(self, chunk_size: int = 200) -> int:
        if not self._vec:
            return 0
        payload = []
        for edge in self.graph.edges:
            edge_key = (
                f"{edge.source_table}->{edge.target_table}->"
                f"{edge.sql_type}->{edge.relation_type}"
            )
            edge_id = hashlib.md5(edge_key.encode("utf-8")).hexdigest()
            payload.append((
                edge_id,
                edge.source_table,
                edge.target_table,
                edge.relation_type,
                edge.sql_type or "",
                edge.sql_preview or "",
                edge.edge_source or ("audit_log" if edge.sql_type in {"INSERT", "CREATE"} else "vanna_sql"),
                edge.freq,
            ))

        inserted = 0
        for start in range(0, len(payload), chunk_size):
            chunk = payload[start:start + chunk_size]
            values_sql = ",".join(["(%s,%s,%s,%s,%s,%s,%s,%s)"] * len(chunk))
            args = []
            for item in chunk:
                args.extend(item)
            inserted += self._vec.execute_write(
                f"""
                INSERT INTO vanna_store.vanna_lineage
                    (edge_id, source_table, target_table, relation_type,
                     sql_type, sql_preview, source, freq)
                VALUES {values_sql}
                """,
                args,
            )
        logger.info(f"[Lineage] 已写入结构化血缘 {inserted} 条")
        return inserted

    def query_lineage_context(self, table_names: List[str], depth: int = 2) -> List[dict]:
        if not self._vec or not table_names:
            return []

        normalized_names = []
        for name in table_names:
            lowered = (name or "").strip().lower()
            if lowered and lowered not in normalized_names:
                normalized_names.append(lowered)
        if not normalized_names:
            return []

        placeholders = ",".join(["%s"] * len(normalized_names))
        rows = self._vec.execute(
            f"""
            SELECT source_table, target_table, relation_type, sql_type, sql_preview, source, freq
            FROM vanna_store.vanna_lineage
            WHERE source_table IN ({placeholders})
               OR target_table IN ({placeholders})
            ORDER BY freq DESC, source_table ASC, target_table ASC
            LIMIT 500
            """,
            normalized_names + normalized_names,
        )

        graph = LineageGraph()
        for row in rows:
            # 用 load_edge 直接恢复 freq，避免 O(freq) 循环
            graph.load_edge(
                row["source_table"],
                row["target_table"],
                row.get("sql_preview", ""),
                row.get("sql_type", ""),
                row.get("relation_type", "table_lineage"),
                row.get("source", ""),
                freq=max(1, int(row.get("freq") or 1)),
            )

        result = []
        for table_name in normalized_names:
            upstream = sorted(graph.get_upstream_tables(table_name, depth=depth))
            downstream = sorted(graph.get_downstream_tables(table_name, depth=depth))
            if not upstream and not downstream:
                continue
            result.append({
                "table_name": table_name,
                "upstream_tables": upstream,
                "downstream_tables": downstream,
                "summary": (
                    f"核心表 {table_name}；"
                    f"上游 {len(upstream)} 张: {', '.join(upstream) or '无'}；"
                    f"下游 {len(downstream)} 张: {', '.join(downstream) or '无'}"
                ),
            })
        return result

    def load_graph_from_table(self) -> int:
        if not self._vec:
            return 0
        rows = self._vec.execute(
            """
            SELECT source_table, target_table, relation_type, sql_type, sql_preview, source, freq
            FROM vanna_store.vanna_lineage
            """
        )
        self.graph = LineageGraph()
        for row in rows:
            # 用 load_edge 直接恢复 freq，避免 O(freq) 循环
            self.graph.load_edge(
                row["source_table"],
                row["target_table"],
                row.get("sql_preview", ""),
                row.get("sql_type", ""),
                row.get("relation_type", "table_lineage"),
                row.get("source", ""),
                freq=max(1, int(row.get("freq") or 1)),
            )
        logger.info("[Lineage] 已从 vanna_lineage 加载 %s 条边", len(rows))
        return len(rows)

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
