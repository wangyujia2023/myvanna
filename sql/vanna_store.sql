-- ==========================================================================
-- Vanna Skill 向量知识库 DDL
-- 执行: mysql -h 10.26.20.3 -P 19030 -u root < sql/vanna_store.sql
-- ==========================================================================

CREATE DATABASE IF NOT EXISTS vanna_store;
USE vanna_store;

-- ── 向量知识库主表 ────────────────────────────────────────────────────────
-- DROP TABLE IF EXISTS vanna_embeddings;

CREATE TABLE IF NOT EXISTS vanna_embeddings (
    id              BIGINT          NOT NULL    COMMENT '主键（应用侧生成）',
    content_type    VARCHAR(20)     NOT NULL    COMMENT '类型：sql / ddl / doc',
    question        TEXT                        COMMENT '自然语言问题（sql类型填写）',
    content         TEXT            NOT NULL    COMMENT 'SQL语句 / DDL / 文档内容',
    embedding       ARRAY<FLOAT>    NOT NULL    COMMENT '1024维向量（Qwen text-embedding-v3）',
    source          VARCHAR(50)                 COMMENT '来源：audit_log/manual/schema/feedback',
    db_name         VARCHAR(100)                COMMENT '所属数据库',
    table_names     TEXT                        COMMENT '涉及的表名（逗号分隔）',
    use_count       INT             DEFAULT 0   COMMENT '历史命中次数',
    quality_score   FLOAT           DEFAULT 1.0 COMMENT '质量评分（0-1，由人工或自动打分）',
    created_at      DATETIME        DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    updated_at      DATETIME        DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
    INDEX ann_idx (embedding) USING ANN PROPERTIES (
        "index_type"  = "hnsw",
        "metric_type" = "inner_product",
        "dim"         = "1024",
        "quantizer"   = "sq8"
    )
) ENGINE=OLAP
DUPLICATE KEY(id)
COMMENT 'Vanna Text-to-SQL 向量知识库'
DISTRIBUTED BY HASH(id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);

-- ── 主键说明 ────────────────────────────────────────────────────────────────
-- 当前由应用侧生成 BIGINT id；若后续 Doris 版本和建表策略支持自增，可再切回。

-- ── 用户反馈记录表 ────────────────────────────────────────────────────────
-- DROP TABLE IF EXISTS vanna_feedback;

CREATE TABLE IF NOT EXISTS vanna_feedback (
    id              BIGINT          NOT NULL    COMMENT '主键',
    session_id      VARCHAR(64)                 COMMENT '会话ID',
    question        TEXT            NOT NULL    COMMENT '用户原始问题',
    generated_sql   TEXT            NOT NULL    COMMENT '系统生成的SQL',
    corrected_sql   TEXT                        COMMENT '用户修正后的SQL（如有）',
    is_correct      BOOLEAN         NOT NULL    COMMENT '结果是否正确',
    embedding_id    BIGINT                      COMMENT '对应向量库记录ID',
    user_tag        VARCHAR(64)                 COMMENT '用户标签/角色',
    created_at      DATETIME        DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
UNIQUE KEY(id)
COMMENT 'Vanna 用户反馈记录'
DISTRIBUTED BY HASH(id) BUCKETS 2
PROPERTIES ("replication_num" = "1");

-- ── 调用链日志表（可选，持久化 trace）────────────────────────────────────────
-- DROP TABLE IF EXISTS vanna_trace_log;

CREATE TABLE IF NOT EXISTS vanna_trace_log (
    trace_id        VARCHAR(32)     NOT NULL    COMMENT 'Trace ID',
    question        TEXT                        COMMENT '用户问题',
    final_sql       TEXT                        COMMENT '最终生成的SQL',
    status          VARCHAR(16)                 COMMENT 'ok / error',
    model_used      VARCHAR(64)                 COMMENT '使用的模型',
    total_ms        FLOAT                       COMMENT '总耗时(ms)',
    error_msg       TEXT                        COMMENT '错误信息',
    steps_json      TEXT                        COMMENT '步骤链路JSON',
    created_at      DATETIME        DEFAULT CURRENT_TIMESTAMP
) ENGINE=OLAP
DUPLICATE KEY(trace_id)
COMMENT 'Vanna 调用链持久化日志'
DISTRIBUTED BY HASH(trace_id) BUCKETS 2
PROPERTIES (
    "replication_num" = "1"
);

-- ==========================================================================
-- 验证
-- ==========================================================================
-- SHOW TABLES FROM vanna_store;
-- DESCRIBE vanna_store.vanna_embeddings;
