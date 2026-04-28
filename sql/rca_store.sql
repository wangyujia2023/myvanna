CREATE DATABASE IF NOT EXISTS rca_store;
USE rca_store;

CREATE TABLE IF NOT EXISTS rca_nodes (
    node_id       BIGINT       NOT NULL COMMENT '节点主键',
    node_name     VARCHAR(128) NOT NULL COMMENT '变量名，如 gmv/traffic_uv/city_code/promotion_flag',
    node_type     VARCHAR(32)  NOT NULL COMMENT 'metric/dimension/segment/external/latent',
    title         VARCHAR(256) COMMENT '显示名',
    description   TEXT         COMMENT '业务说明',
    cube_ref      VARCHAR(128) COMMENT '关联 Cube measure/dimension/segment 名称',
    expression    TEXT         COMMENT '可选表达式或取数说明',
    enabled       TINYINT      DEFAULT 1,
    version       INT          DEFAULT 1,
    updated_at    DATETIME     DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
UNIQUE KEY(node_id)
COMMENT 'RCA 变量节点表：指标、维度、分段、外部因素统一抽象为节点'
DISTRIBUTED BY HASH(node_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS rca_edges (
    edge_id          BIGINT       NOT NULL COMMENT '边主键',
    source_node      VARCHAR(128) NOT NULL COMMENT '影响方节点',
    target_node      VARCHAR(128) NOT NULL COMMENT '被影响节点',
    edge_type        VARCHAR(32)  DEFAULT 'driver' COMMENT 'causal/driver/formula/correlation/constraint',
    direction        VARCHAR(32)  DEFAULT 'unknown' COMMENT 'positive/negative/non_monotonic/unknown',
    prior_strength   DOUBLE       DEFAULT 0.0 COMMENT '长期先验影响强度，-1 到 1，不等于本次贡献度',
    confidence       DOUBLE       DEFAULT 0.5 COMMENT '先验可信度，0 到 1',
    lag              VARCHAR(32)  DEFAULT 'P0D' COMMENT 'ISO-8601 duration，如 P0D/P1D/P7D',
    condition_json   TEXT         COMMENT '生效条件 JSON',
    evidence_json    TEXT         COMMENT '证据来源 JSON：expert/regression/experiment/history',
    enabled          TINYINT      DEFAULT 1,
    version          INT          DEFAULT 1,
    updated_at       DATETIME     DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
UNIQUE KEY(edge_id)
COMMENT 'RCA 有向影响边表：Metric Driver Graph / Causal Graph 的边'
DISTRIBUTED BY HASH(edge_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS rca_causal_specs (
    spec_id                BIGINT       NOT NULL COMMENT '因果验证配置主键',
    treatment_node         VARCHAR(128) NOT NULL COMMENT 'DoWhy treatment',
    outcome_node           VARCHAR(128) NOT NULL COMMENT 'DoWhy outcome',
    common_causes_json     TEXT         COMMENT '混杂变量列表 JSON',
    instruments_json       TEXT         COMMENT '工具变量列表 JSON',
    effect_modifiers_json  TEXT         COMMENT '效应调节变量列表 JSON',
    graph_gml              TEXT         COMMENT '可选 GML 因果图，优先于 named variable sets',
    estimator              VARCHAR(128) DEFAULT 'backdoor.linear_regression',
    refuters_json          TEXT         COMMENT 'refuter 配置 JSON',
    enabled                TINYINT      DEFAULT 1,
    version                INT          DEFAULT 1,
    updated_at             DATETIME     DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
UNIQUE KEY(spec_id)
COMMENT 'RCA DoWhy 因果验证配置表'
DISTRIBUTED BY HASH(spec_id) BUCKETS 2
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS rca_profiles (
    profile_id          BIGINT       NOT NULL COMMENT '归因分析 Profile 主键',
    profile_name        VARCHAR(128) NOT NULL COMMENT 'Profile 名称',
    metric_name         VARCHAR(128) NOT NULL COMMENT '目标指标',
    default_dimensions_json TEXT     COMMENT '默认归因维度列表 JSON',
    default_baseline    VARCHAR(64)  DEFAULT 'previous_period' COMMENT 'previous_period/last_month/yoy',
    min_contribution    DOUBLE       DEFAULT 0.02 COMMENT '最小展示贡献度阈值',
    max_depth           INT          DEFAULT 2 COMMENT '组合根因最大深度',
    algorithm           VARCHAR(64)  DEFAULT 'adtributor' COMMENT 'adtributor/squeeze/auto',
    enabled             TINYINT      DEFAULT 1,
    description         TEXT,
    updated_at          DATETIME     DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
UNIQUE KEY(profile_id)
COMMENT 'RCA 归因 Profile：指标默认维度、基线和算法策略'
DISTRIBUTED BY HASH(profile_id) BUCKETS 2
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS rca_runs (
    run_id              VARCHAR(64)  NOT NULL COMMENT '一次归因运行 ID',
    question            TEXT         COMMENT '用户问题',
    metric_name         VARCHAR(128) COMMENT '目标指标',
    status              VARCHAR(32)  DEFAULT 'running',
    plan_json           TEXT         COMMENT '归因计划 JSON',
    candidates_json     TEXT         COMMENT '候选根因 JSON',
    causal_results_json TEXT         COMMENT '因果验证结果 JSON',
    report_text         TEXT         COMMENT 'LLM 报告',
    created_at          DATETIME     DEFAULT CURRENT_TIMESTAMP,
    updated_at          DATETIME     DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
UNIQUE KEY(run_id)
COMMENT 'RCA 归因运行记录'
DISTRIBUTED BY HASH(run_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS rca_run_candidates (
    candidate_id          BIGINT       NOT NULL COMMENT '候选根因主键',
    run_id                VARCHAR(64)  NOT NULL,
    rank_no               INT          NOT NULL,
    candidate_type        VARCHAR(32)  DEFAULT 'dimension_value' COMMENT 'dimension_value/combination/metric_driver/external',
    candidate_json        TEXT         NOT NULL COMMENT '候选根因描述 JSON',
    runtime_contribution  DOUBLE       COMMENT '本次贡献度',
    prior_score           DOUBLE       COMMENT '先验得分',
    causal_score          DOUBLE       COMMENT '因果验证得分',
    final_score           DOUBLE       COMMENT '最终排序分',
    created_at            DATETIME     DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
UNIQUE KEY(candidate_id)
COMMENT 'RCA 每次运行的候选根因明细'
DISTRIBUTED BY HASH(candidate_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

INSERT INTO rca_nodes
    (node_id, node_name, node_type, title, description, cube_ref, expression, enabled)
VALUES
    (101, 'gmv', 'metric', 'GMV', '销售总额/交易额', 'gmv', 'SUM(orders.apportion_amt)', 1),
    (102, 'order_count', 'metric', '订单量', '订单笔数', 'order_count', 'COUNT(DISTINCT orders.order_id)', 1),
    (103, 'buyer_count', 'metric', '下单人数', '下单用户数', 'buyer_count', 'COUNT(DISTINCT orders.user_id)', 1),
    (104, 'aov', 'metric', '客单价', 'GMV / 下单人数', 'aov', 'gmv / buyer_count', 1),
    (201, 'city_code', 'dimension', '城市', '城市维度', 'city_code', 'cities.city_code/cities.city_name', 1),
    (202, 'store_type', 'dimension', '门店类型', '直营/加盟等门店类型', 'store_type', 'stores.store_type', 1),
    (203, 'member_type', 'dimension', '会员类型', 'PLUS会员/普通会员', 'member_type', 'member_types.member_type', 1),
    (204, 'category_1_id', 'dimension', '一级类目', '商品一级类目', 'category_1_id', 'orders.category_1_id', 1),
    (301, 'traffic_uv', 'external', '访客数', '流量驱动因子，待接入流量指标', '', '', 1),
    (302, 'conversion_rate', 'metric', '转化率', '访客到成交转化率，待接入指标', '', '', 1),
    (303, 'stock_available_rate', 'external', '库存可售率', '库存/供给侧影响因子，待接入库存数据', '', '', 1),
    (304, 'promotion_flag', 'external', '促销活动', '活动/折扣影响因子', '', '', 1);

INSERT INTO rca_edges
    (edge_id, source_node, target_node, edge_type, direction, prior_strength, confidence, lag, evidence_json, enabled)
VALUES
    (1001, 'traffic_uv', 'gmv', 'driver', 'positive', 0.35, 0.60, 'P0D', '{"source":"expert_seed"}', 1),
    (1002, 'conversion_rate', 'gmv', 'driver', 'positive', 0.30, 0.60, 'P0D', '{"source":"expert_seed"}', 1),
    (1003, 'aov', 'gmv', 'driver', 'positive', 0.25, 0.60, 'P0D', '{"source":"expert_seed"}', 1),
    (1004, 'stock_available_rate', 'gmv', 'driver', 'positive', 0.20, 0.50, 'P0D', '{"source":"expert_seed"}', 1),
    (1005, 'promotion_flag', 'gmv', 'causal', 'positive', 0.18, 0.45, 'P0D', '{"source":"expert_seed"}', 1),
    (1006, 'order_count', 'gmv', 'formula', 'positive', 0.40, 0.80, 'P0D', '{"source":"metric_formula"}', 1),
    (1007, 'buyer_count', 'order_count', 'driver', 'positive', 0.35, 0.65, 'P0D', '{"source":"expert_seed"}', 1);

INSERT INTO rca_causal_specs
    (spec_id, treatment_node, outcome_node, common_causes_json, instruments_json, effect_modifiers_json, estimator, refuters_json, enabled)
VALUES
    (2001, 'promotion_flag', 'gmv', '["city_code","store_type","member_type","category_1_id"]', '[]', '["member_type","city_code"]', 'backdoor.linear_regression', '["placebo_treatment_refuter","bootstrap_refuter","random_common_cause"]', 1);

INSERT INTO rca_profiles
    (profile_id, profile_name, metric_name, default_dimensions_json, default_baseline, min_contribution, max_depth, algorithm, enabled, description)
VALUES
    (3001, 'gmv_default', 'gmv', '["city_code","store_type","member_type","category_1_id"]', 'previous_period', 0.02, 2, 'adtributor', 1, 'GMV 默认归因 Profile');
