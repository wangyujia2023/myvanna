USE cube_store;

CREATE TABLE IF NOT EXISTS cube_models (
    model_id        BIGINT          NOT NULL COMMENT '模型主键',
    cube_name       VARCHAR(128)    NOT NULL COMMENT 'Cube 名称',
    title           VARCHAR(256)    COMMENT '显示名',
    sql_table       VARCHAR(256)    COMMENT '物理表名',
    sql_expression  TEXT            COMMENT '可选自定义 SQL',
    data_source     VARCHAR(64)     DEFAULT 'default',
    public_flag     TINYINT         DEFAULT 1,
    visible         TINYINT         DEFAULT 1,
    version         INT             DEFAULT 1,
    updated_at      DATETIME        DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
UNIQUE KEY(model_id)
COMMENT 'Cube 模型主表'
DISTRIBUTED BY HASH(model_id) BUCKETS 2
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS cube_measures (
    measure_id      BIGINT          NOT NULL,
    cube_name       VARCHAR(128)    NOT NULL,
    measure_name    VARCHAR(128)    NOT NULL,
    title           VARCHAR(256)    NOT NULL,
    description     TEXT,
    sql_expr        TEXT            NOT NULL,
    measure_type    VARCHAR(64)     NOT NULL COMMENT 'sum/count/avg/number/countDistinct',
    format          VARCHAR(64)     DEFAULT 'number',
    drill_members_json TEXT,
    visible         TINYINT         DEFAULT 1,
    version         INT             DEFAULT 1,
    updated_at      DATETIME        DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
UNIQUE KEY(measure_id)
COMMENT 'Cube 指标定义'
DISTRIBUTED BY HASH(measure_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS cube_dimensions (
    dimension_id        BIGINT          NOT NULL,
    cube_name           VARCHAR(128)    NOT NULL,
    dimension_name      VARCHAR(128)    NOT NULL,
    title               VARCHAR(256)    NOT NULL,
    description         TEXT,
    sql_expr            TEXT            NOT NULL,
    dimension_type      VARCHAR(64)     NOT NULL COMMENT 'string/number/time/boolean',
    primary_key_flag    TINYINT         DEFAULT 0,
    enum_mapping_json   TEXT,
    hierarchy_json      TEXT,
    visible             TINYINT         DEFAULT 1,
    version             INT             DEFAULT 1,
    updated_at          DATETIME        DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
UNIQUE KEY(dimension_id)
COMMENT 'Cube 维度定义'
DISTRIBUTED BY HASH(dimension_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS cube_dimension_values (
    value_id          BIGINT          NOT NULL,
    cube_name         VARCHAR(128)    NOT NULL COMMENT '所属 Cube',
    dimension_name    VARCHAR(128)    NOT NULL COMMENT '所属维度',
    value_code        VARCHAR(512)    NOT NULL COMMENT '真实入库值/过滤值',
    value_label       VARCHAR(512)    COMMENT '业务显示名',
    aliases_json      TEXT            COMMENT '同义词/别名数组，如 ["北京市","帝都"]',
    source            VARCHAR(64)     DEFAULT 'manual' COMMENT 'manual/scan/import',
    source_table      VARCHAR(256)    COMMENT '采集来源表',
    source_column     VARCHAR(256)    COMMENT '采集来源列',
    usage_count       BIGINT          DEFAULT 0 COMMENT '采样出现次数',
    visible           TINYINT         DEFAULT 1,
    version           INT             DEFAULT 1,
    updated_at        DATETIME        DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
UNIQUE KEY(value_id)
COMMENT 'Cube 维度枚举值与别名表'
DISTRIBUTED BY HASH(value_id) BUCKETS 4
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS cube_joins (
    join_id          BIGINT          NOT NULL,
    cube_name        VARCHAR(128)    NOT NULL,
    target_cube      VARCHAR(128)    NOT NULL,
    relationship     VARCHAR(64)     NOT NULL COMMENT 'one_to_many/many_to_one/one_to_one',
    join_type        VARCHAR(32)     DEFAULT 'LEFT',
    join_sql         TEXT            NOT NULL,
    visible          TINYINT         DEFAULT 1,
    version          INT             DEFAULT 1,
    updated_at       DATETIME        DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
UNIQUE KEY(join_id)
COMMENT 'Cube 关联定义'
DISTRIBUTED BY HASH(join_id) BUCKETS 2
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS cube_segments (
    segment_id       BIGINT          NOT NULL,
    cube_name        VARCHAR(128)    NOT NULL,
    segment_name     VARCHAR(128)    NOT NULL,
    title            VARCHAR(256)    NOT NULL,
    description      TEXT,
    filter_sql       TEXT            NOT NULL,
    visible          TINYINT         DEFAULT 1,
    version          INT             DEFAULT 1,
    updated_at       DATETIME        DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
UNIQUE KEY(segment_id)
COMMENT 'Cube 业务分段定义'
DISTRIBUTED BY HASH(segment_id) BUCKETS 2
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS cube_sql_templates (
    template_id      BIGINT          NOT NULL,
    template_name    VARCHAR(128)    NOT NULL,
    template_type    VARCHAR(64)     NOT NULL,
    title            VARCHAR(256)    NOT NULL,
    template_sql     TEXT            NOT NULL,
    params_json      TEXT,
    visible          TINYINT         DEFAULT 1,
    version          INT             DEFAULT 1,
    updated_at       DATETIME        DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
UNIQUE KEY(template_id)
COMMENT 'Cube SQL 模板与片段'
DISTRIBUTED BY HASH(template_id) BUCKETS 2
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS cube_meta (
    meta_id          BIGINT          NOT NULL,
    scope            VARCHAR(64)     NOT NULL,
    meta_key         VARCHAR(128)    NOT NULL,
    meta_value       TEXT,
    visible          TINYINT         DEFAULT 1,
    version          INT             DEFAULT 1,
    updated_at       DATETIME        DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
UNIQUE KEY(meta_id)
COMMENT 'Cube 扩展元数据'
DISTRIBUTED BY HASH(meta_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS cube_model_versions (
    version_id       BIGINT          NOT NULL,
    version_no       BIGINT          NOT NULL,
    checksum         VARCHAR(64)     NOT NULL,
    status           VARCHAR(32)     DEFAULT 'active',
    remark           TEXT,
    updated_at       DATETIME        DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
UNIQUE KEY(version_id)
COMMENT 'Cube 模型版本表'
DISTRIBUTED BY HASH(version_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
