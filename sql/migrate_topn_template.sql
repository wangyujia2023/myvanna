-- ============================================================
-- TopN 组内排名模板系统 迁移脚本
-- 修改内容：
--   1. 创建 vanna_sql_templates 表（存储 SQL 模板）
--   2. 插入 topn_per_group 模板
--   3. vanna_semantic_metrics 新增 query_pattern / template_params_json 列
--   4. 插入 store_gmv_topn 指标（每个城市门店销售额 Top3）
-- 执行：mysql -h 10.26.20.3 -P 19030 -u root semantic_store < migrate_topn_template.sql
-- ============================================================

USE semantic_store;

-- ────────────────────────────────────────────────────────────
-- 1. 创建 vanna_sql_templates 表
-- ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS vanna_sql_templates (
  template_id    VARCHAR(64)   NOT NULL   COMMENT '主键 UUID（去连字符）',
  name           VARCHAR(128)  NOT NULL   COMMENT '模板名，如 topn_per_group',
  pattern_type   VARCHAR(64)   NOT NULL   COMMENT '模式类型，与 query_pattern 字段对应',
  label          VARCHAR(256)  DEFAULT '' COMMENT '中文名称',
  description    TEXT                     COMMENT '模板说明',
  sql_template   TEXT          NOT NULL   COMMENT '含 {占位符} 的 SQL 模板',
  params_schema  TEXT                     COMMENT 'JSON：参数列表及说明',
  db_name        VARCHAR(64)   DEFAULT '' COMMENT '适用数据库，空=通用',
  is_active      TINYINT       DEFAULT 1  COMMENT '1=启用',
  created_at     DATETIME      DEFAULT NOW()
) ENGINE=OLAP
  UNIQUE KEY(template_id)
  DISTRIBUTED BY HASH(template_id) BUCKETS 1
  PROPERTIES("replication_num" = "1");

-- ────────────────────────────────────────────────────────────
-- 2. 插入 topn_per_group 通用模板
--    占位符由 SQLSynthesizer 在运行时填充，此处仅作文档存档
-- ────────────────────────────────────────────────────────────
DELETE FROM vanna_sql_templates WHERE name = 'topn_per_group';

INSERT INTO vanna_sql_templates
  (template_id, name, pattern_type, label, description,
   sql_template, params_schema, db_name)
VALUES (
  REPLACE(UUID(),'-',''),
  'topn_per_group',
  'topn_per_group',
  '组内 TopN',
  '对每个分组维度内，按指标值排序取 TopN 行。使用 ROW_NUMBER() OVER (PARTITION BY {partition_alias} ORDER BY {metric_expr} {order_dir}) 窗口函数，外层 WHERE rk <= {top_n} 截断。',
  'SELECT {outer_cols}
FROM (
  SELECT {inner_cols},
         {metric_expr} AS {metric_name},
         ROW_NUMBER() OVER (
           PARTITION BY {partition_alias}
           ORDER BY {metric_expr} {order_dir}
         ) AS rk
  FROM   {primary_table} {primary_alias}
  {join_clauses}
  {where_clause}
  GROUP BY {inner_group_by}
) _ranked
WHERE  rk <= {top_n}
ORDER BY {partition_alias}, rk',
  '[
    {"name":"partition_dim",  "desc":"分组维度 name，如 city_dim，指定 PARTITION BY 列"},
    {"name":"top_n",          "desc":"每组保留行数，默认 3"},
    {"name":"order_dir",      "desc":"排序方向，DESC（默认）或 ASC"}
  ]',
  ''
);

-- ────────────────────────────────────────────────────────────
-- 3. vanna_semantic_metrics 新增 query_pattern / template_params_json
--    Doris ALTER TABLE ADD COLUMN（幂等：若已存在会报错，可忽略）
-- ────────────────────────────────────────────────────────────
ALTER TABLE vanna_semantic_metrics
  ADD COLUMN query_pattern VARCHAR(64) DEFAULT '' COMMENT 'topn_per_group | pivot | 空=普通聚合';

ALTER TABLE vanna_semantic_metrics
  ADD COLUMN template_params_json TEXT COMMENT '模板参数 JSON，如 {"partition_dim":"city_dim","top_n":3}';

-- ────────────────────────────────────────────────────────────
-- 4. 插入 store_gmv_topn 指标
-- ────────────────────────────────────────────────────────────
DELETE FROM vanna_semantic_metrics
WHERE name = 'store_gmv_topn'
  AND (db_name = 'retail_dw' OR IFNULL(db_name,'') = '');

INSERT INTO vanna_semantic_metrics
  (metric_id, name, label, description,
   metric_type, complexity,
   expression, primary_table, primary_alias,
   extra_joins_json,
   time_column,
   numerator_expr, denominator_expr,
   output_format, unit,
   compatible_dimensions_json,
   synonyms, tags, db_name,
   query_pattern, template_params_json)
VALUES (
  REPLACE(UUID(),'-',''),
  'store_gmv_topn',
  '门店销售额组内 TopN',
  '按城市分组，门店销售额排名前 N（默认 Top3）',
  'simple', 'normal',
  'SUM(o.apportion_amt)',
  'dwd_trade_order_wide',
  'o',
  '[{"table":"dim_store_info","alias":"ds","join_type":"LEFT JOIN","on":"o.store_id = ds.store_id"}]',
  'o.dt',
  '', '',
  'currency', '元',
  '["time_day","time_month","time_quarter","city_dim","store_dim"]',
  '每个城市销售额排名,城市门店销售排名,各城市Top门店,城市门店GMV排名,门店销售排行,城市销售前三门店,各地区销售额前三,城市门店销售额前N,TopN门店,门店销售排行榜',
  'store,geo,revenue,topn',
  'retail_dw',
  'topn_per_group',
  '{"partition_dim":"city_dim","top_n":3,"order_dir":"DESC"}'
);

-- ────────────────────────────────────────────────────────────
-- 验证
-- ────────────────────────────────────────────────────────────
SELECT name, query_pattern, template_params_json
FROM vanna_semantic_metrics
WHERE name = 'store_gmv_topn';

SELECT name, pattern_type, label, LEFT(sql_template, 120) AS tpl_preview
FROM vanna_sql_templates
WHERE name = 'topn_per_group';
