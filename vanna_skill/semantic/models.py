"""
语义层数据模型
4 种节点类型：EntityDef / DimensionDef / MetricDef / BusinessDef
以及查询过程中产生的中间结构：SemanticPlan / QueryTask / QueryPlan
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional


# ─────────────────────────────────────────────────────────────────────────────
# 节点定义
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class JoinDef:
    """描述一张需要 JOIN 进来的表"""
    table: str
    alias: str
    join_type: str = "LEFT JOIN"    # LEFT JOIN | INNER JOIN | JOIN
    on: str = ""                    # JOIN 条件，{fact_alias} 会在合成时替换


@dataclass
class EntityDef:
    """
    实体节点：业务中的核心对象（门店、商品、用户…）
    对应 dim_xxx 维度表，是 Dimension 的归属对象。
    """
    name: str                           # 英文唯一标识，如 store
    label: str                          # 中文名，如 门店
    description: str = ""
    primary_table: str = ""             # 主表，如 dim_store_info
    primary_key: str = ""               # 主键字段，如 store_id
    display_key: str = ""               # 展示字段，如 store_name
    searchable_fields: List[str] = field(default_factory=list)
    synonyms: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)

    # ── 图关系（由 SemanticGraph 填充）
    dimensions: List[str] = field(default_factory=list)  # 关联的 dimension names

    @property
    def node_id(self) -> str:
        return f"entity:{self.name}"


@dataclass
class DimensionDef:
    """
    维度节点：分析视角（时间维度、门店维度、商品维度…）
    """
    name: str                           # 如 time_month / store_dim
    label: str                          # 如 月份 / 门店维度
    description: str = ""
    dim_type: str = "attribute"         # time | attribute | entity_ref
    entity: Optional[str] = None        # 关联的 entity name（entity_ref 类型）

    # ── 时间维度专用
    grain: Optional[str] = None         # day | month | quarter | year
    expression: str = ""                # DATE_FORMAT({time_col}, '%Y-%m')，{time_col} 运行时替换
    alias: str = ""                     # SELECT 中的别名，如 stat_month

    # ── 实体引用维度专用（store_dim / sku_dim 等）
    join: Optional[JoinDef] = None      # 需要 JOIN 的表
    select_fields: List[str] = field(default_factory=list)  # 从 join 表取的字段

    synonyms: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)

    @property
    def node_id(self) -> str:
        return f"dimension:{self.name}"

    @property
    def select_alias(self) -> str:
        return self.alias or self.name


@dataclass
class MetricDef:
    """
    指标节点：可度量的业务值（GMV / 净收入 / 退款率…）
    """
    name: str                           # 如 net_revenue
    label: str                          # 如 净收入
    description: str = ""
    metric_type: str = "simple"         # simple | ratio | derived | composite
    complexity: str = "normal"          # normal | high（high 优先走 LLM 路径）

    # ── SQL 合成核心字段
    expression: str = ""                # 聚合表达式，如 SUM(o.apportion_amt)
    primary_source: Optional[Dict[str, str]] = None
    # {"table": "dwd_trade_order_wide", "alias": "o"}

    extra_joins: List[JoinDef] = field(default_factory=list)
    time_column: str = ""               # 时间过滤字段，如 o.dt

    # ── ratio 类型拆分（可选，用于精确计算）
    numerator_expr: str = ""
    denominator_expr: str = ""

    # ── 输出格式
    output_format: str = "number"       # number | percent | currency
    unit: str = ""                      # 元 | % | 件

    # ── 兼容性
    compatible_dimensions: List[str] = field(default_factory=list)

    synonyms: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)

    # ── 模板查询模式（topn_per_group / pivot / 空=普通聚合）
    query_pattern: str = ""
    # template_params 示例：{"partition_dim": "city_dim", "top_n": 3, "order_dir": "DESC"}
    template_params: Dict = field(default_factory=dict)

    # ── 图关系（由 SemanticGraph 填充）
    entity_deps: List[str] = field(default_factory=list)

    @property
    def node_id(self) -> str:
        return f"metric:{self.name}"

    @property
    def primary_table(self) -> str:
        return (self.primary_source or {}).get("table", "")

    @property
    def primary_alias(self) -> str:
        return (self.primary_source or {}).get("alias", "t")

    def is_high_complexity(self) -> bool:
        return self.complexity == "high"


@dataclass
class BusinessDef:
    """
    业务域节点：高层次问题聚类（销售概览 / 用户分析…）
    用于意图模糊时快速召回相关指标。
    """
    name: str                           # 如 sales_overview
    label: str                          # 如 销售概览
    description: str = ""
    related_metrics: List[str] = field(default_factory=list)
    related_dimensions: List[str] = field(default_factory=list)
    typical_questions: List[str] = field(default_factory=list)  # 正则/关键词列表
    default_dimensions: List[str] = field(default_factory=list)
    default_sort: str = ""

    synonyms: List[str] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)

    @property
    def node_id(self) -> str:
        return f"business:{self.name}"


# ─────────────────────────────────────────────────────────────────────────────
# 查询过程中间结构
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class IntentPlan:
    """
    IntentUnderstandingAgent 输出：用户意图的结构化描述
    """
    intent_type: str = "data_query"
    # data_query | metric_explain | root_cause | schema_lookup | attribution | invalid

    business_domain: str = "unknown"
    # sales_overview | user_analysis | product_performance | unknown

    complexity: str = "simple"
    # simple | compound | attribution

    time_hint: str = ""                 # 如 "2026-04" / "最近30天" / ""
    entity_hints: List[str] = field(default_factory=list)   # ["门店", "商品"]
    action_hints: List[str] = field(default_factory=list)   # ["排名", "对比"]
    raw_question: str = ""
    normalized_query: str = ""
    rejection_reason: str = ""          # intent_type=invalid 时填写


@dataclass
class FilterCondition:
    """过滤条件"""
    column: str                         # 字段名或别名
    operator: str = "="                 # = | >= | <= | BETWEEN | IN | LIKE
    value: Any = None
    value2: Any = None                  # BETWEEN 时的第二个值

    def to_sql(self) -> str:
        if self.operator == "BETWEEN" and self.value2 is not None:
            return f"{self.column} BETWEEN '{self.value}' AND '{self.value2}'"
        if self.operator == "IN" and isinstance(self.value, list):
            vals = ", ".join(f"'{v}'" for v in self.value)
            return f"{self.column} IN ({vals})"
        if self.operator == "LIKE":
            return f"{self.column} LIKE '%{self.value}%'"
        return f"{self.column} {self.operator} '{self.value}'"


@dataclass
class TimeScope:
    """标准化时间范围。"""
    scope_type: str = ""               # day | month | year | range | recent_days | quarter
    grain: str = ""                    # day | month | year
    start: str = ""
    end: str = ""
    label: str = ""
    raw_text: str = ""


@dataclass
class ComparisonSpec:
    """对比/增长分析定义。"""
    mode: str = ""                     # none | mom | yoy | wow | previous_period
    enabled: bool = False
    compare_start: str = ""
    compare_end: str = ""
    output_style: str = "growth_rate"  # growth_rate | delta | both
    label: str = ""


@dataclass
class QuerySpec:
    """
    SQL 编译器直接消费的结构化查询规范。
    LLM 只负责填槽位，不负责生成 SQL。
    """
    metrics: List[str] = field(default_factory=list)
    dimensions: List[str] = field(default_factory=list)
    filters: List[FilterCondition] = field(default_factory=list)
    order_by: List[Dict[str, str]] = field(default_factory=list)
    limit: Optional[int] = 20
    analysis_type: str = "aggregate"  # aggregate | compare | trend | ranking
    time_scope: Optional[TimeScope] = None
    comparison: Optional[ComparisonSpec] = None
    unresolved_parts: List[str] = field(default_factory=list)
    notes: List[str] = field(default_factory=list)


@dataclass
class SemanticPlan:
    """
    SemanticParseAgent 输出：把自然语言映射成结构化语义查询
    """
    metrics: List[str] = field(default_factory=list)       # ["net_revenue", "vip_buyer_rate"]
    dimensions: List[str] = field(default_factory=list)    # ["time_month", "store_dim"]
    filters: List[FilterCondition] = field(default_factory=list)
    order_by: List[Dict[str, str]] = field(default_factory=list)
    # [{"field": "net_revenue", "direction": "DESC"}]
    limit: int = 20

    coverage_score: float = 0.0
    # 0~1，指标+维度能在 SemanticCatalog 中命中的比例

    unresolved_parts: List[str] = field(default_factory=list)
    # 未能映射到任何已知指标/维度的片段，供 LLM 兜底路径使用

    intent_plan: Optional[IntentPlan] = None
    query_spec: Optional[QuerySpec] = None


@dataclass
class QueryTask:
    """
    QueryPlanAgent 拆解出的单个执行任务
    """
    task_id: str = "t1"
    task_type: str = "sql_query"
    # sql_query | attribution（P2） | aggregate | compare

    metrics: List[str] = field(default_factory=list)
    dimensions: List[str] = field(default_factory=list)
    filters: List[FilterCondition] = field(default_factory=list)
    order_by: List[Dict[str, str]] = field(default_factory=list)
    limit: int = 20
    query_spec: Optional[QuerySpec] = None

    depends_on: List[str] = field(default_factory=list)    # 上游 task_id
    description: str = ""


@dataclass
class QueryPlan:
    """
    QueryPlanAgent 输出：完整执行计划（DAG）
    """
    tasks: List[QueryTask] = field(default_factory=list)
    execution_mode: str = "sequential"  # sequential | parallel
    semantic_plan: Optional[SemanticPlan] = None


@dataclass
class SemanticResult:
    """
    SemanticPipeline 最终输出
    """
    question: str = ""
    intent: IntentPlan = field(default_factory=IntentPlan)
    semantic_plan: Optional[SemanticPlan] = None
    query_plan: Optional[QueryPlan] = None

    # 主路径结果
    sql: str = ""
    path: str = "semantic"              # semantic | lc_fallback
    guard_ok: bool = False

    # 错误/拒绝
    error: str = ""
    rejection: str = ""

    # 调试
    trace: Dict[str, Any] = field(default_factory=dict)
    steps: List[Dict[str, Any]] = field(default_factory=list)
