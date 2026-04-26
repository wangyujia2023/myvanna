"""
SchemaScanner：从 Doris 自动生成语义定义草稿

数据来源（三路合并）：
  1. information_schema.tables  → 表名、表注释、行数
  2. information_schema.columns → 列名、类型、列注释
  3. __internal_schema.audit_log → SQL 使用模式分析
     - GROUP BY 出现的列  → 维度候选
     - SUM/COUNT 出现的列 → 指标候选
     - JOIN 模式          → 表关系
     - SELECT 别名        → 中文 label 候选

输出：ScanResult（草稿），由调用方决定是否写入 semantic_store。
"""
from __future__ import annotations

import json
import logging
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

if TYPE_CHECKING:
    from ..doris_client import DorisClient

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# 数据结构：扫描结果草稿
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ColInfo:
    """单列信息"""
    table: str
    name: str
    data_type: str          # bigint / varchar / decimal / datetime ...
    comment: str = ""       # information_schema.columns.COLUMN_COMMENT
    # 从 audit_log 统计
    sum_count: int = 0      # 被 SUM() 包含的次数
    count_count: int = 0    # 被 COUNT()/COUNT(DISTINCT) 包含的次数
    groupby_count: int = 0  # 出现在 GROUP BY 的次数
    where_count: int = 0    # 出现在 WHERE 的次数
    aliases: List[str] = field(default_factory=list)  # SELECT x AS alias 中收集到的 alias


@dataclass
class TableInfo:
    """单表信息"""
    name: str
    comment: str = ""
    row_count: int = 0
    columns: List[ColInfo] = field(default_factory=list)
    # 从 audit_log 统计
    query_count: int = 0    # 出现在 FROM/JOIN 的次数
    join_targets: List[str] = field(default_factory=list)  # 经常被 JOIN 的表


@dataclass
class ScanProposal:
    """扫描生成的单个语义节点草稿"""
    node_type: str          # entity | dimension | metric
    name: str               # 英文唯一标识（程序生成）
    label: str              # 中文显示名
    description: str = ""
    data: dict = field(default_factory=dict)   # 完整的 YAML-compatible dict
    confidence: float = 0.5  # 0~1，越高越可信
    source: str = ""         # 生成来源说明


@dataclass
class ScanResult:
    """完整扫描结果"""
    db_name: str
    table_infos: List[TableInfo] = field(default_factory=list)
    proposals: List[ScanProposal] = field(default_factory=list)
    # 统计
    tables_scanned: int = 0
    columns_scanned: int = 0
    audit_logs_analyzed: int = 0
    warnings: List[str] = field(default_factory=list)

    def to_yaml_dict(self) -> dict:
        """把草稿转成可直接导入的 YAML dict 结构"""
        entities, dimensions, metrics, business = [], [], [], []
        for p in self.proposals:
            if p.node_type == "entity":
                entities.append(p.data)
            elif p.node_type == "dimension":
                dimensions.append(p.data)
            elif p.node_type == "metric":
                metrics.append(p.data)
        return {
            "version": "1.0",
            "db_name": self.db_name,
            "entities": entities,
            "dimensions": dimensions,
            "metrics": metrics,
            "business": business,
        }


# ─────────────────────────────────────────────────────────────────────────────
# 正则表达式常量
# ─────────────────────────────────────────────────────────────────────────────

# 时间列名特征
_TIME_COL_PATTERN = re.compile(
    r"\b(dt|date|stat_date|stat_day|stat_month|create_time|created_at|"
    r"update_time|updated_at|order_date|pay_date|refund_date|event_time|"
    r"biz_date|ds|p_date|partition_date)\b",
    re.IGNORECASE,
)

# 指标字段名特征（聚合有意义的字段）
_METRIC_COL_PATTERN = re.compile(
    r"(amt|amount|price|fee|cost|revenue|income|profit|discount|"
    r"cnt|count|num|qty|quantity|times|pv|uv|gmv|sales)\b",
    re.IGNORECASE,
)

# 维度字段名特征（分组有意义的字段）
_DIM_COL_PATTERN = re.compile(
    r"(_id$|_code$|_type$|_level$|_status$|_name$|_category$|"
    r"city|region|province|area|channel|platform|brand|grade)",
    re.IGNORECASE,
)

# 数值类型（指标候选）
_NUMERIC_TYPES = {"bigint", "int", "tinyint", "smallint", "decimal",
                  "double", "float", "largeint"}

# SQL 解析正则
_RE_SUM    = re.compile(r"\bSUM\s*\(\s*(?:DISTINCT\s+)?(?:\w+\.)?(\w+)\s*\)", re.I)
_RE_COUNT  = re.compile(r"\bCOUNT\s*\(\s*(?:DISTINCT\s+)?(?:\w+\.)?(\w+)\s*\)", re.I)
_RE_AVG    = re.compile(r"\bAVG\s*\(\s*(?:\w+\.)?(\w+)\s*\)", re.I)
_RE_GROUPBY = re.compile(
    r"\bGROUP\s+BY\b(.*?)(?:\bHAVING\b|\bORDER\b|\bLIMIT\b|$)",
    re.I | re.S,
)
_RE_SELECT_ALIAS = re.compile(
    r"(?:SUM|COUNT|AVG|MAX|MIN|ROUND|COALESCE)\s*\([^)]+\)\s+(?:AS\s+)?([`'\"]?[\u4e00-\u9fa5\w]+[`'\"]?)",
    re.I,
)
_RE_JOIN = re.compile(
    r"\bJOIN\s+(\w+)\s+(?:\w+\s+)?ON\b",
    re.I,
)
_RE_FROM = re.compile(r"\bFROM\s+(\w+)\s", re.I)

# 表名前缀分类
_FACT_TABLE_PREFIX = ("dwd_", "fct_", "fact_", "dws_", "ads_", "ods_")
_DIM_TABLE_PREFIX  = ("dim_", "dimension_", "d_")


# ─────────────────────────────────────────────────────────────────────────────
# SchemaScanner 主类
# ─────────────────────────────────────────────────────────────────────────────

class SchemaScanner:
    """
    从 information_schema + audit_log 自动生成语义定义草稿。

    Parameters
    ----------
    biz_client    : 连接 retail_dw（用于 information_schema 查询）
    audit_client  : 连接 __internal_schema（可与 biz_client 相同，只需不指定 database）
    db_name       : 要扫描的业务库名，如 retail_dw
    llm           : 可选，QwenClient，用于 label/description 增强
    """

    def __init__(
        self,
        biz_client: "DorisClient",
        audit_client: "DorisClient",
        db_name: str = "retail_dw",
        llm=None,
    ):
        self._biz = biz_client
        self._audit = audit_client
        self._db_name = db_name
        self._llm = llm

    # ─────────────────────────────────────────────────────────────────────────
    # 公共入口
    # ─────────────────────────────────────────────────────────────────────────

    def scan(
        self,
        include_tables: Optional[List[str]] = None,   # None = 全部表
        audit_limit: int = 5000,                       # 最多分析最近 N 条 SQL
        min_confidence: float = 0.3,                   # 过滤低置信度草稿
    ) -> ScanResult:
        """
        执行扫描，返回 ScanResult（草稿，不写 DB）。
        """
        result = ScanResult(db_name=self._db_name)

        # Step 1: 读取表和列信息
        logger.info("[SchemaScanner] Step1: 读取 information_schema ...")
        table_infos = self._load_schema(include_tables)
        result.table_infos = table_infos
        result.tables_scanned = len(table_infos)
        result.columns_scanned = sum(len(t.columns) for t in table_infos)
        logger.info(
            "[SchemaScanner] 读取完成: %d 张表, %d 列",
            result.tables_scanned, result.columns_scanned,
        )

        # Step 2: 分析 audit_log
        logger.info("[SchemaScanner] Step2: 分析 audit_log (limit=%d) ...", audit_limit)
        col_stats, table_stats = self._analyze_audit_log(audit_limit)
        result.audit_logs_analyzed = audit_limit

        # 把 audit 统计回填到 table_infos
        self._merge_audit_stats(table_infos, col_stats, table_stats)

        # Step 3: 生成草稿
        logger.info("[SchemaScanner] Step3: 生成语义定义草稿 ...")
        proposals = self._generate_proposals(table_infos)
        result.proposals = [p for p in proposals if p.confidence >= min_confidence]

        logger.info(
            "[SchemaScanner] 扫描完成: %d 条草稿（confidence >= %.1f）",
            len(result.proposals), min_confidence,
        )
        return result

    # ─────────────────────────────────────────────────────────────────────────
    # Step 1：information_schema 读取
    # ─────────────────────────────────────────────────────────────────────────

    def _load_schema(
        self, include_tables: Optional[List[str]]
    ) -> List[TableInfo]:
        # 读取表信息
        table_filter = ""
        params: list = [self._db_name]
        if include_tables:
            placeholders = ",".join(["%s"] * len(include_tables))
            table_filter = f"AND TABLE_NAME IN ({placeholders})"
            params.extend(include_tables)

        try:
            rows = self._biz.execute(
                f"""
                SELECT TABLE_NAME, IFNULL(TABLE_COMMENT,'') AS TABLE_COMMENT,
                       IFNULL(TABLE_ROWS, 0) AS TABLE_ROWS
                FROM information_schema.tables
                WHERE TABLE_SCHEMA = %s
                  AND TABLE_TYPE = 'BASE TABLE'
                  {table_filter}
                ORDER BY TABLE_NAME
                """,
                params,
            )
        except Exception as e:
            logger.warning("[SchemaScanner] 读取 tables 失败: %s", e)
            return []

        table_map: Dict[str, TableInfo] = {}
        for r in rows:
            t = TableInfo(
                name=r["TABLE_NAME"],
                comment=r.get("TABLE_COMMENT") or "",
                row_count=int(r.get("TABLE_ROWS") or 0),
            )
            table_map[t.name] = t

        # 读取列信息
        try:
            col_rows = self._biz.execute(
                f"""
                SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE,
                       IFNULL(COLUMN_COMMENT,'') AS COLUMN_COMMENT,
                       ORDINAL_POSITION
                FROM information_schema.columns
                WHERE TABLE_SCHEMA = %s
                  {f"AND TABLE_NAME IN ({','.join(['%s']*len(include_tables))})" if include_tables else ""}
                ORDER BY TABLE_NAME, ORDINAL_POSITION
                """,
                [self._db_name] + (include_tables or []),
            )
        except Exception as e:
            logger.warning("[SchemaScanner] 读取 columns 失败: %s", e)
            col_rows = []

        for r in col_rows:
            tname = r["TABLE_NAME"]
            if tname not in table_map:
                continue
            table_map[tname].columns.append(ColInfo(
                table=tname,
                name=r["COLUMN_NAME"],
                data_type=r["DATA_TYPE"].lower(),
                comment=r.get("COLUMN_COMMENT") or "",
            ))

        return list(table_map.values())

    # ─────────────────────────────────────────────────────────────────────────
    # Step 2：audit_log 分析
    # ─────────────────────────────────────────────────────────────────────────

    def _analyze_audit_log(
        self, limit: int
    ) -> Tuple[Dict[str, ColInfo], Dict[str, dict]]:
        """
        从 audit_log 分析 SQL 使用模式。

        Returns
        -------
        col_stats   : {col_name → ColInfo（含 sum_count 等统计）}
        table_stats : {table_name → {"query_count": int, "join_targets": Counter}}
        """
        col_stats: Dict[str, ColInfo] = defaultdict(lambda: ColInfo("", "", ""))
        table_stats: Dict[str, dict] = defaultdict(lambda: {"query_count": 0, "join_targets": Counter()})

        try:
            rows = self._audit.execute(
                """
                SELECT stmt
                FROM __internal_schema.audit_log
                WHERE stmt LIKE '%SELECT%'
                  AND LENGTH(stmt) > 20
                  AND stmt NOT LIKE '%audit_log%'
                  AND stmt NOT LIKE '%information_schema%'
                ORDER BY `time` DESC
                LIMIT %s
                """,
                (limit,),
            )
        except Exception as e:
            logger.warning("[SchemaScanner] 读取 audit_log 失败: %s", e)
            return col_stats, table_stats

        for r in rows:
            sql = r.get("stmt") or ""
            if not sql:
                continue
            self._parse_sql(sql, col_stats, table_stats)

        return col_stats, table_stats

    def _parse_sql(
        self,
        sql: str,
        col_stats: Dict[str, ColInfo],
        table_stats: Dict[str, dict],
    ):
        """解析单条 SQL，更新统计。"""
        sql_upper = sql.upper()

        # SUM(col) / SUM(alias.col)
        for col in _RE_SUM.findall(sql):
            col_lower = col.lower()
            col_stats[col_lower].sum_count += 1

        # COUNT(DISTINCT col)
        for col in _RE_COUNT.findall(sql):
            col_lower = col.lower()
            col_stats[col_lower].count_count += 1

        # GROUP BY col1, col2, ...
        for m in _RE_GROUPBY.finditer(sql):
            group_block = m.group(1)
            # 提取 GROUP BY 后的列名（去掉 alias. 前缀，去掉数字引用）
            for token in re.split(r"[,\s]+", group_block):
                token = token.strip()
                token = re.sub(r"^\w+\.", "", token)   # 去别名前缀
                token = re.sub(r"[`'\"]", "", token)
                if token and not token.isdigit() and re.match(r"^\w+$", token):
                    col_stats[token.lower()].groupby_count += 1

        # SELECT 别名（SUM(...) AS label）
        for alias in _RE_SELECT_ALIAS.findall(sql):
            alias = re.sub(r"[`'\"]", "", alias).strip()
            if alias and len(alias) <= 20:
                # 把 alias 关联到 SUM/COUNT 的列
                for col in _RE_SUM.findall(sql):
                    col_stats[col.lower()].aliases.append(alias)
                    break   # 只取第一个

        # FROM / JOIN 表频率
        for tname in _RE_FROM.findall(sql):
            tname_lower = tname.lower()
            if tname_lower not in ("dual", ""):
                table_stats[tname_lower]["query_count"] += 1

        # JOIN 关联对
        join_tables = _RE_JOIN.findall(sql)
        from_tables = _RE_FROM.findall(sql)
        for ft in from_tables:
            for jt in join_tables:
                ft_lower, jt_lower = ft.lower(), jt.lower()
                if ft_lower != jt_lower:
                    table_stats[ft_lower]["join_targets"][jt_lower] += 1

    def _merge_audit_stats(
        self,
        table_infos: List[TableInfo],
        col_stats: Dict[str, ColInfo],
        table_stats: Dict[str, dict],
    ):
        """把 audit 统计回填到 ColInfo 和 TableInfo。"""
        for ti in table_infos:
            ts = table_stats.get(ti.name.lower())
            if ts:
                ti.query_count = ts["query_count"]
                ti.join_targets = [t for t, _ in ts["join_targets"].most_common(5)]
            for col in ti.columns:
                cs = col_stats.get(col.name.lower())
                if cs:
                    col.sum_count = cs.sum_count
                    col.count_count = cs.count_count
                    col.groupby_count = cs.groupby_count
                    col.where_count = cs.where_count
                    col.aliases = list(set(cs.aliases))[:3]  # 最多 3 个

    # ─────────────────────────────────────────────────────────────────────────
    # Step 3：生成草稿
    # ─────────────────────────────────────────────────────────────────────────

    def _generate_proposals(
        self, table_infos: List[TableInfo]
    ) -> List[ScanProposal]:
        proposals: List[ScanProposal] = []

        for ti in table_infos:
            is_dim = any(ti.name.lower().startswith(p) for p in _DIM_TABLE_PREFIX)
            is_fact = any(ti.name.lower().startswith(p) for p in _FACT_TABLE_PREFIX)

            # ── 维度表 → EntityDef ────────────────────────────────────────
            if is_dim:
                entity_name = self._table_to_entity_name(ti.name)
                label = ti.comment or self._guess_label(ti.name)
                pk_col = self._guess_pk(ti)
                display_col = self._guess_display_col(ti)

                proposals.append(ScanProposal(
                    node_type="entity",
                    name=entity_name,
                    label=label,
                    description=ti.comment,
                    confidence=0.8,
                    source=f"information_schema: {ti.name} 是维度表",
                    data={
                        "name": entity_name,
                        "label": label,
                        "description": ti.comment,
                        "primary_table": ti.name,
                        "primary_key": pk_col,
                        "display_key": display_col,
                        "searchable_fields": self._guess_searchable_fields(ti),
                        "synonyms": self._generate_synonyms(ti.name, ti.comment),
                        "tags": ["dim"],
                    },
                ))

                # 同时生成对应的 entity_ref 维度
                dim_name = f"{entity_name}_dim"
                join_on = f"{{fact_alias}}.{pk_col} = d_{entity_name[:4]}.{pk_col}" if pk_col else ""
                join_alias = f"d_{entity_name[:4]}"

                proposals.append(ScanProposal(
                    node_type="dimension",
                    name=dim_name,
                    label=f"{label}维度",
                    description=f"按{label}分组分析",
                    confidence=0.75,
                    source=f"自动生成: {ti.name} 维度表对应的分析视角",
                    data={
                        "name": dim_name,
                        "label": f"{label}维度",
                        "dim_type": "entity_ref",
                        "entity": entity_name,
                        "join": {
                            "table": ti.name,
                            "alias": join_alias,
                            "join_type": "LEFT JOIN",
                            "on": join_on,
                        },
                        "select_fields": ([display_col, pk_col] if display_col and pk_col
                                         else self._guess_searchable_fields(ti)[:3]),
                        "synonyms": self._generate_synonyms(ti.name, ti.comment),
                        "tags": ["dim"],
                    },
                ))

            # ── 事实表 → 指标候选 ──────────────────────────────────────────
            if is_fact or (not is_dim and ti.query_count > 0):
                time_col = self._guess_time_column(ti)
                main_alias = self._table_to_alias(ti.name)

                for col in ti.columns:
                    metric_data = self._try_generate_metric(
                        col, ti, main_alias, time_col
                    )
                    if metric_data:
                        proposals.append(metric_data)

        # 生成时间维度（通用）
        proposals.extend(self._generate_time_dimensions())

        return proposals

    def _try_generate_metric(
        self,
        col: ColInfo,
        ti: TableInfo,
        main_alias: str,
        time_col: str,
    ) -> Optional[ScanProposal]:
        """
        判断一列是否适合生成指标草稿，如果是则返回 ScanProposal，否则 None。
        """
        col_name = col.name.lower()

        # 跳过时间列、主键列
        if _TIME_COL_PATTERN.search(col_name):
            return None
        if col_name.endswith("_id") and col.data_type in ("bigint", "varchar", "int"):
            # 仅当 COUNT(DISTINCT) 频繁出现时才生成指标
            if col.count_count < 2:
                return None

        # 判断是否适合聚合
        is_numeric = col.data_type in _NUMERIC_TYPES
        is_metric_name = bool(_METRIC_COL_PATTERN.search(col_name))
        is_sum_used = col.sum_count > 0
        is_count_used = col.count_count > 0

        if not (is_numeric or is_metric_name or is_sum_used or is_count_used):
            return None

        # 决定聚合函数
        if col_name.endswith("_id") or col_name in ("order_id", "user_id", "sku_id"):
            agg = f"COUNT(DISTINCT {{alias}}.{col.name})"
            agg_label_suffix = "数"
        elif is_sum_used or re.search(r"(amt|amount|price|fee|revenue|gmv|sales)", col_name, re.I):
            agg = f"SUM({{alias}}.{col.name})"
            agg_label_suffix = ""
        elif re.search(r"(cnt|count|num|qty|quantity|times|pv|uv)", col_name, re.I):
            agg = f"SUM({{alias}}.{col.name})"
            agg_label_suffix = ""
        else:
            agg = f"SUM({{alias}}.{col.name})"
            agg_label_suffix = ""

        # label：优先用列注释，其次用 audit_log 别名，最后 guess
        label = col.comment or (col.aliases[0] if col.aliases else "") or self._guess_col_label(col.name)
        if agg_label_suffix and not label.endswith(agg_label_suffix):
            label = label + agg_label_suffix

        metric_name = f"{ti.name.lower().replace('-', '_')}__{col_name}"
        # 缩短 name：去掉表名前缀
        short_name = self._shorten_metric_name(ti.name, col.name)

        # 计算置信度
        confidence = 0.4
        if col.comment:     confidence += 0.2
        if is_sum_used:     confidence += 0.2
        if is_count_used:   confidence += 0.1
        if is_metric_name:  confidence += 0.15
        confidence = min(confidence, 0.95)

        return ScanProposal(
            node_type="metric",
            name=short_name,
            label=label,
            description=col.comment or f"{ti.name}.{col.name}",
            confidence=confidence,
            source=(
                f"table={ti.name}, col={col.name}, type={col.data_type}, "
                f"sum_used={col.sum_count}, groupby_used={col.groupby_count}"
            ),
            data={
                "name": short_name,
                "label": label,
                "description": col.comment or "",
                "metric_type": "simple",
                "expression": agg,
                "primary_source": {
                    "table": ti.name,
                    "alias": main_alias,
                },
                "time_column": f"{main_alias}.{time_col}" if time_col else "",
                "output_format": self._guess_output_format(col.name),
                "unit": self._guess_unit(col.name, col.comment),
                "compatible_dimensions": [],  # 后续可补充
                "synonyms": ([col.comment] if col.comment and col.comment != label else []) + col.aliases,
                "tags": self._guess_tags(ti.name, col.name),
            },
        )

    def _generate_time_dimensions(self) -> List[ScanProposal]:
        """生成通用时间维度（day/month/quarter/year），总是置信度高。"""
        dims = [
            ("time_day",     "日期",   "day",     "{time_col}",                              "stat_day"),
            ("time_month",   "月份",   "month",   "DATE_FORMAT({time_col}, '%Y-%m')",         "stat_month"),
            ("time_quarter", "季度",   "quarter", "CONCAT(YEAR({time_col}),'-Q',QUARTER({time_col}))", "stat_quarter"),
            ("time_year",    "年份",   "year",    "YEAR({time_col})",                        "stat_year"),
        ]
        proposals = []
        for name, label, grain, expr, alias in dims:
            proposals.append(ScanProposal(
                node_type="dimension",
                name=name,
                label=label,
                confidence=0.95,
                source="自动生成：通用时间维度",
                data={
                    "name": name,
                    "label": label,
                    "dim_type": "time",
                    "grain": grain,
                    "expression": expr,
                    "alias": alias,
                    "synonyms": {
                        "time_day":     ["日期", "天", "按天", "每天", "每日"],
                        "time_month":   ["月份", "月", "按月", "每月", "月度"],
                        "time_quarter": ["季度", "季", "按季", "每季"],
                        "time_year":    ["年份", "年", "按年", "每年", "年度"],
                    }[name],
                    "tags": ["time"],
                },
            ))
        return proposals

    # ─────────────────────────────────────────────────────────────────────────
    # 辅助：命名 / 猜测
    # ─────────────────────────────────────────────────────────────────────────

    def _table_to_entity_name(self, table_name: str) -> str:
        """dim_store_info → store"""
        for prefix in _DIM_TABLE_PREFIX:
            if table_name.lower().startswith(prefix):
                suffix = table_name[len(prefix):]
                # 去掉常见后缀
                for s in ("_info", "_detail", "_base", "_full"):
                    if suffix.endswith(s):
                        suffix = suffix[: -len(s)]
                return suffix.lower()
        return table_name.lower()

    def _table_to_alias(self, table_name: str) -> str:
        """dwd_trade_order_wide → o（取第一个有意义词的首字母）"""
        parts = table_name.lower().split("_")
        for p in reversed(parts):
            if p not in ("dwd", "dws", "ads", "dim", "ods", "fct", "fact",
                         "wide", "info", "detail", "base", "full"):
                return p[0]
        return "t"

    def _guess_label(self, table_name: str) -> str:
        """根据表名猜测中文 label（粗略）"""
        mappings = {
            "order": "订单", "trade": "交易", "refund": "退款", "payment": "支付",
            "store": "门店", "shop": "门店", "user": "用户", "member": "会员",
            "sku": "商品", "product": "商品", "item": "商品",
            "category": "品类", "brand": "品牌",
            "coupon": "优惠券", "marketing": "营销", "campaign": "活动",
            "delivery": "配送", "logistics": "物流",
            "inventory": "库存", "stock": "库存",
        }
        name_lower = table_name.lower()
        for kw, label in mappings.items():
            if kw in name_lower:
                return label
        # 用表名的非前缀部分
        parts = name_lower.split("_")
        for p in _FACT_TABLE_PREFIX + _DIM_TABLE_PREFIX:
            stripped = p.rstrip("_")
            if parts and parts[0] == stripped:
                parts = parts[1:]
        return "_".join(parts[:2]) if parts else table_name

    def _guess_col_label(self, col_name: str) -> str:
        """列名 → 中文 label 猜测"""
        mappings = {
            "apportion_amt": "分摊金额", "amt": "金额", "amount": "金额",
            "price": "单价", "pay_amt": "支付金额", "order_amt": "订单金额",
            "discount_amt": "折扣金额", "refund_amt": "退款金额",
            "gmv": "GMV", "revenue": "收入", "profit": "利润",
            "cnt": "数量", "count": "数量", "num": "数量", "qty": "件数",
            "order_id": "订单ID", "user_id": "用户ID", "sku_id": "商品ID",
            "store_id": "门店ID", "shop_id": "门店ID",
            "pv": "PV", "uv": "UV",
        }
        col_lower = col_name.lower()
        for k, v in mappings.items():
            if col_lower == k or col_lower.endswith(f"_{k}"):
                return v
        # 驼峰/下划线 → 按下划线分词
        words = col_name.split("_")
        return " ".join(w.capitalize() for w in words if w)

    def _guess_pk(self, ti: TableInfo) -> str:
        """猜测主键列名"""
        for col in ti.columns:
            if col.name.lower() in (f"{self._table_to_entity_name(ti.name)}_id",
                                    "id", "pk", f"{ti.name}_id"):
                return col.name
        # 找第一个 _id 结尾的列
        for col in ti.columns:
            if col.name.lower().endswith("_id"):
                return col.name
        return ""

    def _guess_display_col(self, ti: TableInfo) -> str:
        """猜测展示用的名称列"""
        for col in ti.columns:
            if col.name.lower().endswith("_name"):
                return col.name
        return ""

    def _guess_searchable_fields(self, ti: TableInfo) -> List[str]:
        """猜测可搜索字段"""
        fields = []
        for col in ti.columns:
            if (col.name.lower().endswith("_name") or
                col.name.lower().endswith("_type") or
                col.name.lower().endswith("_level") or
                col.name.lower() in ("city", "region", "province", "channel", "brand")):
                fields.append(col.name)
        return fields[:5]

    def _guess_time_column(self, ti: TableInfo) -> str:
        """猜测时间列"""
        # 优先用 audit 中 WHERE 出现最多的时间列
        for col in ti.columns:
            if _TIME_COL_PATTERN.search(col.name.lower()):
                if col.name.lower() in ("dt", "stat_date", "stat_day",
                                        "biz_date", "ds"):
                    return col.name
        # 次选任意时间列
        for col in ti.columns:
            if _TIME_COL_PATTERN.search(col.name.lower()):
                return col.name
        return ""

    def _shorten_metric_name(self, table_name: str, col_name: str) -> str:
        """
        生成简短的 metric name：
        dwd_trade_order_wide + apportion_amt → order_apportion_amt
        """
        # 提取表名的"业务词"（去掉 dwd_/dws_ 等前缀和 _wide/_full 等后缀）
        parts = table_name.lower().split("_")
        skip = {"dwd", "dws", "ads", "ods", "fct", "fact", "dim",
                "wide", "full", "info", "detail", "base"}
        biz_parts = [p for p in parts if p not in skip]
        biz_prefix = "_".join(biz_parts[:2]) if biz_parts else table_name.split("_")[-1]
        return f"{biz_prefix}_{col_name.lower()}"

    def _guess_output_format(self, col_name: str) -> str:
        col = col_name.lower()
        if re.search(r"(amt|amount|price|fee|revenue|gmv|cost|profit)", col):
            return "currency"
        if re.search(r"(rate|ratio|percent|pct)", col):
            return "percent"
        return "number"

    def _guess_unit(self, col_name: str, comment: str) -> str:
        col = col_name.lower()
        text = (col + comment).lower()
        if re.search(r"(amt|amount|price|revenue|gmv|cost|profit)", col):
            return "元"
        if re.search(r"(rate|ratio|pct)", col):
            return "%"
        if re.search(r"(qty|quantity)", col):
            return "件"
        if re.search(r"(cnt|count|num|order)", col):
            return "单"
        return ""

    def _guess_tags(self, table_name: str, col_name: str) -> List[str]:
        tags = []
        t = table_name.lower()
        c = col_name.lower()
        if "refund" in t or "refund" in c:  tags.append("refund")
        if "trade" in t or "order" in t:    tags.append("trade")
        if "user" in t:                     tags.append("user")
        if "sku" in t or "product" in t:    tags.append("sku")
        if "coupon" in t or "marketing" in t: tags.append("marketing")
        if re.search(r"(amt|amount|revenue|gmv)", c): tags.append("revenue")
        return tags

    def _generate_synonyms(self, table_name: str, comment: str) -> List[str]:
        syns = []
        if comment and len(comment) <= 10:
            syns.append(comment)
        label = self._guess_label(table_name)
        if label and label not in syns:
            syns.append(label)
        return syns[:5]
