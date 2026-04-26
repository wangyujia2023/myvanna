-- ==========================================================================
-- 语义层知识图谱 DDL  (Phase 1: Semantic Layer — 4 独立类型表)
-- 执行: mysql -h <host> -P 19030 -u root < sql/vanna_semantic.sql
-- ==========================================================================

USE semantic_store;

-- ── 删除旧版单表（如已存在，迁移前先备份）────────────────────────────────
-- DROP TABLE IF EXISTS vanna_semantic_nodes;

-- ════════════════════════════════════════════════════════════════════════════
-- 1. 实体表（EntityDef）
--    核心业务对象：门店、商品、用户、订单
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS vanna_semantic_entities (
    entity_id               VARCHAR(128)    NOT NULL    COMMENT '节点ID，格式 entity:{name}',
    name                    VARCHAR(128)    NOT NULL    COMMENT '英文唯一标识，如 store',
    label                   VARCHAR(256)    NOT NULL    COMMENT '中文显示名，如 门店',
    description             TEXT                        COMMENT '业务描述',
    primary_table           VARCHAR(256)                COMMENT '主表名，如 dim_store_info',
    primary_key             VARCHAR(128)                COMMENT '主键字段，如 store_id',
    display_key             VARCHAR(128)                COMMENT '展示字段，如 store_name',
    searchable_fields_json  TEXT                        COMMENT '可搜索字段列表（JSON数组）',
    synonyms                TEXT                        COMMENT '同义词（逗号分隔，用于语义匹配）',
    tags                    VARCHAR(512)                COMMENT '标签（逗号分隔）',
    db_name                 VARCHAR(100)                COMMENT '所属业务库，为空表示通用',
    is_active               TINYINT         DEFAULT 1   COMMENT '是否启用',
    version                 INT             DEFAULT 1   COMMENT '版本号，每次更新+1',
    created_at              DATETIME        DEFAULT CURRENT_TIMESTAMP,
    updated_at              DATETIME        DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
UNIQUE KEY(entity_id)
COMMENT '语义实体节点（门店/商品/用户/订单等核心对象）'
DISTRIBUTED BY HASH(entity_id) BUCKETS 2
PROPERTIES ("replication_num" = "1");


-- ════════════════════════════════════════════════════════════════════════════
-- 2. 维度表（DimensionDef）
--    分析视角：时间维度、门店维度、商品维度等
--    dim_type: time | attribute | entity_ref
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS vanna_semantic_dimensions (
    dim_id              VARCHAR(128)    NOT NULL    COMMENT '节点ID，格式 dimension:{name}',
    name                VARCHAR(128)    NOT NULL    COMMENT '英文唯一标识，如 time_month',
    label               VARCHAR(256)    NOT NULL    COMMENT '中文显示名，如 月份',
    description         TEXT                        COMMENT '业务描述',
    dim_type            VARCHAR(32)     DEFAULT 'attribute'
                                                    COMMENT '维度类型: time|attribute|entity_ref',
    entity              VARCHAR(128)                COMMENT '关联实体名（entity_ref 类型使用）',
    -- ── 时间维度专用 ──────────────────────────────────────────────────────
    grain               VARCHAR(32)                 COMMENT '时间粒度: day|month|quarter|year',
    expression          TEXT                        COMMENT 'SQL表达式，{time_col}为占位符',
    alias               VARCHAR(128)                COMMENT 'SELECT中的别名，如 stat_month',
    -- ── 实体引用维度专用（JOIN 信息）──────────────────────────────────────
    join_table          VARCHAR(256)                COMMENT 'JOIN表名，如 dim_store_info',
    join_alias          VARCHAR(64)                 COMMENT 'JOIN表别名，如 s',
    join_type           VARCHAR(32)     DEFAULT 'LEFT JOIN'
                                                    COMMENT 'LEFT JOIN|INNER JOIN|JOIN',
    join_on             TEXT                        COMMENT 'JOIN条件，{fact_alias}为主表别名占位',
    select_fields_json  TEXT                        COMMENT '从JOIN表取的字段列表（JSON数组）',
    -- ── 通用 ──────────────────────────────────────────────────────────────
    synonyms            TEXT                        COMMENT '同义词（逗号分隔）',
    tags                VARCHAR(512)                COMMENT '标签（逗号分隔）',
    db_name             VARCHAR(100)                COMMENT '所属业务库',
    is_active           TINYINT         DEFAULT 1,
    version             INT             DEFAULT 1,
    created_at          DATETIME        DEFAULT CURRENT_TIMESTAMP,
    updated_at          DATETIME        DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
UNIQUE KEY(dim_id)
COMMENT '语义维度节点（时间/门店/商品/用户等分析视角）'
DISTRIBUTED BY HASH(dim_id) BUCKETS 2
PROPERTIES ("replication_num" = "1");


-- ════════════════════════════════════════════════════════════════════════════
-- 3. 指标表（MetricDef）
--    可度量的业务值：GMV、净收入、退款率、复购率等
--    metric_type: simple | ratio | derived | composite
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS vanna_semantic_metrics (
    metric_id               VARCHAR(128)    NOT NULL    COMMENT '节点ID，格式 metric:{name}',
    name                    VARCHAR(128)    NOT NULL    COMMENT '英文唯一标识，如 net_revenue',
    label                   VARCHAR(256)    NOT NULL    COMMENT '中文显示名，如 净收入',
    description             TEXT                        COMMENT '业务描述',
    metric_type             VARCHAR(32)     DEFAULT 'simple'
                                                        COMMENT '指标类型: simple|ratio|derived|composite',
    complexity              VARCHAR(16)     DEFAULT 'normal'
                                                        COMMENT '复杂度: normal|high（high优先走LLM路径）',
    -- ── SQL 合成核心 ───────────────────────────────────────────────────────
    expression              TEXT                        COMMENT '聚合表达式，如 SUM(o.apportion_amt)',
    primary_table           VARCHAR(256)                COMMENT '主表名，如 dwd_trade_order_wide',
    primary_alias           VARCHAR(64)     DEFAULT 't' COMMENT '主表别名，如 o',
    extra_joins_json        TEXT                        COMMENT '额外JOIN列表（JSON数组，含table/alias/join_type/on）',
    time_column             VARCHAR(256)                COMMENT '时间过滤字段，如 o.dt',
    -- ── ratio 类型专用 ──────────────────────────────────────────────────
    numerator_expr          TEXT                        COMMENT 'ratio分子聚合表达式',
    denominator_expr        TEXT                        COMMENT 'ratio分母聚合表达式',
    -- ── 输出格式 ────────────────────────────────────────────────────────
    output_format           VARCHAR(32)     DEFAULT 'number'
                                                        COMMENT '输出格式: number|percent|currency',
    unit                    VARCHAR(64)                 COMMENT '单位，如 元|%|件',
    -- ── 兼容维度 ────────────────────────────────────────────────────────
    compatible_dimensions_json TEXT                     COMMENT '兼容的维度name列表（JSON数组）',
    -- ── 通用 ────────────────────────────────────────────────────────────
    synonyms                TEXT                        COMMENT '同义词（逗号分隔）',
    tags                    VARCHAR(512)                COMMENT '标签（逗号分隔）',
    db_name                 VARCHAR(100)                COMMENT '所属业务库',
    is_active               TINYINT         DEFAULT 1,
    version                 INT             DEFAULT 1,
    created_at              DATETIME        DEFAULT CURRENT_TIMESTAMP,
    updated_at              DATETIME        DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
UNIQUE KEY(metric_id)
COMMENT '语义指标节点（GMV/净收入/退款率等可度量业务值）'
DISTRIBUTED BY HASH(metric_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");


-- ════════════════════════════════════════════════════════════════════════════
-- 4. 业务域表（BusinessDef）
--    高层次问题聚类：销售概览、用户分析、退款分析等
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS vanna_semantic_businesses (
    biz_id                  VARCHAR(128)    NOT NULL    COMMENT '节点ID，格式 business:{name}',
    name                    VARCHAR(128)    NOT NULL    COMMENT '英文唯一标识，如 sales_overview',
    label                   VARCHAR(256)    NOT NULL    COMMENT '中文显示名，如 销售概览',
    description             TEXT                        COMMENT '业务描述',
    related_metrics_json    TEXT                        COMMENT '关联指标name列表（JSON数组）',
    related_dimensions_json TEXT                        COMMENT '关联维度name列表（JSON数组）',
    typical_questions_json  TEXT                        COMMENT '典型问题/关键词列表（JSON数组）',
    default_dimensions_json TEXT                        COMMENT '默认展示维度name列表（JSON数组）',
    default_sort            VARCHAR(128)                COMMENT '默认排序，如 net_revenue DESC',
    synonyms                TEXT                        COMMENT '同义词（逗号分隔）',
    tags                    VARCHAR(512)                COMMENT '标签（逗号分隔）',
    db_name                 VARCHAR(100)                COMMENT '所属业务库',
    is_active               TINYINT         DEFAULT 1,
    version                 INT             DEFAULT 1,
    created_at              DATETIME        DEFAULT CURRENT_TIMESTAMP,
    updated_at              DATETIME        DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
UNIQUE KEY(biz_id)
COMMENT '语义业务域节点（销售/用户/退款/商品等业务聚类）'
DISTRIBUTED BY HASH(biz_id) BUCKETS 2
PROPERTIES ("replication_num" = "1");


-- ════════════════════════════════════════════════════════════════════════════
-- 5. 语义关系表（有向图边，跨类型关联）
--    relation 类型说明：
--      entity_has_dimension   实体拥有某个维度
--      metric_uses_entity     指标依赖某个实体（主表）
--      metric_by_dimension    指标可以按某维度切割/分组
--      business_has_metric    业务域包含某个指标
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS vanna_semantic_edges (
    edge_id         VARCHAR(128)    NOT NULL    COMMENT '边ID = MD5(from_node+to_node+relation)',
    from_node       VARCHAR(128)    NOT NULL    COMMENT '起始节点 node_id，如 metric:net_revenue',
    to_node         VARCHAR(128)    NOT NULL    COMMENT '终止节点 node_id，如 entity:store',
    relation        VARCHAR(64)     NOT NULL    COMMENT '关系类型',
    weight          FLOAT           DEFAULT 1.0 COMMENT '权重（0~1，用于多指标排序）',
    meta_json       TEXT                        COMMENT '附加元数据（JSON）',
    created_at      DATETIME        DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
UNIQUE KEY(edge_id)
COMMENT '语义知识图谱边（有向关系）'
DISTRIBUTED BY HASH(edge_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");


-- ════════════════════════════════════════════════════════════════════════════
-- 6. 语义版本快照表（便于回滚和审计）
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS vanna_semantic_snapshots (
    snapshot_id     VARCHAR(64)     NOT NULL    COMMENT '快照ID = MD5(timestamp+operator)',
    operator        VARCHAR(128)                COMMENT '操作人/操作来源',
    entity_count    INT             DEFAULT 0   COMMENT '实体数',
    dimension_count INT             DEFAULT 0   COMMENT '维度数',
    metric_count    INT             DEFAULT 0   COMMENT '指标数',
    business_count  INT             DEFAULT 0   COMMENT '业务域数',
    yaml_content    TEXT                        COMMENT '完整 YAML 快照',
    remark          TEXT                        COMMENT '备注',
    created_at      DATETIME        DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
UNIQUE KEY(snapshot_id)
COMMENT '语义层版本快照（支持回滚与审计）'
DISTRIBUTED BY HASH(snapshot_id) BUCKETS 2
PROPERTIES ("replication_num" = "1");


-- ════════════════════════════════════════════════════════════════════════════
-- 7. 语义 SQL RAG 表（参数占位后的语义模板样本）
--    用于 SemanticPipeline 的独立召回，不复用 LangChain 的 SQL skill 逻辑
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS vanna_semantic_sql_rag (
    rag_id                   BIGINT          NOT NULL COMMENT '样本ID',
    source_sql_id            BIGINT                      COMMENT '来源 vanna_sql.id',
    raw_question             TEXT                        COMMENT '原始问题',
    canonical_question       TEXT                        COMMENT '参数占位后的问题模板',
    sql_text                 TEXT                        COMMENT '对应 SQL',
    source                   VARCHAR(64)                 COMMENT 'feedback / feedback_corrected',
    db_name                  VARCHAR(100)                COMMENT '业务库',
    quality_score            FLOAT           DEFAULT 0   COMMENT '质量分',
    embedding                ARRAY<FLOAT>                COMMENT 'canonical_question 向量',
    canonical_hash           VARCHAR(64)                 COMMENT '模板哈希',
    metadata_json            TEXT                        COMMENT '附加元数据（JSON）',
    created_at               DATETIME        DEFAULT CURRENT_TIMESTAMP,
    updated_at               DATETIME        DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
DUPLICATE KEY(rag_id)
COMMENT 'Semantic 独立 SQL RAG 样本表（参数占位向量检索）'
DISTRIBUTED BY HASH(rag_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");


-- ==========================================================================
-- 验证
-- SHOW TABLES FROM vanna_store LIKE 'vanna_semantic%';
-- SELECT COUNT(*) FROM vanna_store.vanna_semantic_entities;
-- SELECT COUNT(*) FROM vanna_store.vanna_semantic_dimensions;
-- SELECT COUNT(*) FROM vanna_store.vanna_semantic_metrics;
-- SELECT COUNT(*) FROM vanna_store.vanna_semantic_businesses;
-- SELECT relation, COUNT(*) FROM vanna_store.vanna_semantic_edges GROUP BY relation;
-- ==========================================================================
