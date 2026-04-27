-- ============================================================
-- semantic_store 迁移脚本
-- 修复内容：
--   1. 修复 category_dim / user_type_dim 空 JOIN ON 条件
--   2. 修复 vip_buyer_count / vip_gmv_rate extra_joins ON 条件
--   3. 新增 plus_gmv 指标（PLUS会员消费金额）
--   4. 新增 city_dim / region_dim 维度（支持城市/大区过滤）
--   5. user_type_dim 补充 filter_column 信息
-- 执行：mysql -h 10.26.20.3 -P 19030 -u root semantic_store < migrate_semantic_store.sql
-- ============================================================

USE semantic_store;

-- ────────────────────────────────────────────────────────────
-- 1. 修复 category_dim JOIN ON 条件
-- ────────────────────────────────────────────────────────────
UPDATE vanna_semantic_dimensions
SET join_on = '{fact_alias}.sku_id = dsku.sku_id'
WHERE name = 'category_dim'
  AND (db_name = 'retail_dw' OR IFNULL(db_name,'') = '')
  AND (join_on IS NULL OR join_on = '');

-- ────────────────────────────────────────────────────────────
-- 2. 修复 user_type_dim：JOIN ON + select_fields + filter_column
-- ────────────────────────────────────────────────────────────
UPDATE vanna_semantic_dimensions
SET
  join_on           = '{fact_alias}.user_id = u.user_id',
  select_fields_json = '[\"CASE WHEN u.is_plus_vip = 1 THEN ''PLUS会员'' ELSE ''普通会员'' END AS member_type\", \"user_level\"]',
  description       = '按用户类型（PLUS会员/普通会员）分组，关联 dim_user_info'
WHERE name = 'user_type_dim'
  AND (db_name = 'retail_dw' OR IFNULL(db_name,'') = '');

-- ────────────────────────────────────────────────────────────
-- 3. 新增 city_dim（城市维度）
-- ────────────────────────────────────────────────────────────
DELETE FROM vanna_semantic_dimensions
WHERE name = 'city_dim'
  AND (db_name = 'retail_dw' OR IFNULL(db_name,'') = '');

INSERT INTO vanna_semantic_dimensions
  (dim_id, name, label, description,
   dim_type, entity, grain,
   expression, alias,
   join_table, join_alias, join_type, join_on,
   select_fields_json, synonyms, tags, db_name)
VALUES (
  REPLACE(UUID(),'-',''),
  'city_dim',
  '城市维度',
  '按城市/地区分组，基于 city_code 字段',
  'attribute', '', '',
  '{fact_alias}.city_code',
  'city_code',
  '', '', '', '',
  '[]',
  '城市,地区,地域,区域,城市维度,北京,上海,广州,深圳',
  'geo',
  'retail_dw'
);

-- ────────────────────────────────────────────────────────────
-- 4. 新增 region_dim（大区维度）
-- ────────────────────────────────────────────────────────────
DELETE FROM vanna_semantic_dimensions
WHERE name = 'region_dim'
  AND (db_name = 'retail_dw' OR IFNULL(db_name,'') = '');

INSERT INTO vanna_semantic_dimensions
  (dim_id, name, label, description,
   dim_type, entity, grain,
   expression, alias,
   join_table, join_alias, join_type, join_on,
   select_fields_json, synonyms, tags, db_name)
VALUES (
  REPLACE(UUID(),'-',''),
  'region_dim',
  '大区维度',
  '按大区（华东/华南/华北等）分组',
  'attribute', '', '',
  '{fact_alias}.region_code',
  'region_code',
  '', '', '', '',
  '[]',
  '大区,区域,华东,华南,华北,华中,西南,东北,地区',
  'geo',
  'retail_dw'
);

-- ────────────────────────────────────────────────────────────
-- 5. 修复 vip_buyer_count extra_joins ON 条件
-- ────────────────────────────────────────────────────────────
UPDATE vanna_semantic_metrics
SET extra_joins_json = '[{"table": "dim_user_info", "alias": "u", "join_type": "LEFT JOIN", "on": "o.user_id = u.user_id"}]'
WHERE name = 'vip_buyer_count'
  AND (db_name = 'retail_dw' OR IFNULL(db_name,'') = '');

-- ────────────────────────────────────────────────────────────
-- 6. 修复 vip_gmv_rate extra_joins ON 条件
-- ────────────────────────────────────────────────────────────
UPDATE vanna_semantic_metrics
SET extra_joins_json = '[{"table": "dim_user_info", "alias": "u", "join_type": "LEFT JOIN", "on": "o.user_id = u.user_id"}]'
WHERE name = 'vip_gmv_rate'
  AND (db_name = 'retail_dw' OR IFNULL(db_name,'') = '');

-- ────────────────────────────────────────────────────────────
-- 7. 新增 plus_gmv 指标（PLUS会员总消费金额）
-- ────────────────────────────────────────────────────────────
DELETE FROM vanna_semantic_metrics
WHERE name = 'plus_gmv'
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
   synonyms, tags, db_name)
VALUES (
  REPLACE(UUID(),'-',''),
  'plus_gmv',
  'PLUS会员消费金额',
  'PLUS会员产生的总消费金额',
  'simple', 'normal',
  'SUM(CASE WHEN u.is_plus_vip = 1 THEN o.apportion_amt ELSE 0 END)',
  'dwd_trade_order_wide',
  'o',
  '[{"table": "dim_user_info", "alias": "u", "join_type": "LEFT JOIN", "on": "o.user_id = u.user_id"}]',
  'o.dt',
  '', '',
  'currency', '元',
  '["time_day","time_month","time_quarter","city_dim","region_dim","store_dim","sku_dim","category_dim"]',
  'PLUS消费,PLUS会员消费,PLUS会员消费金额,PLUS会员总消费,PLUS消费额,PLUS消费金额,VIP消费,VIP消费金额,会员消费,会员消费金额,plus会员消费,PLUS总消费',
  'user,vip,revenue',
  'retail_dw'
);

-- ────────────────────────────────────────────────────────────
-- 8. 修复 net_revenue：补充 extra_joins（关联退款表 r）
-- ────────────────────────────────────────────────────────────
UPDATE vanna_semantic_metrics
SET extra_joins_json = '[{"table":"dwd_trade_refund_fact","alias":"r","join_type":"LEFT JOIN","on":"o.order_id = r.order_id"}]'
WHERE name = 'net_revenue'
  AND (db_name = 'retail_dw' OR IFNULL(db_name,'') = '');

-- ────────────────────────────────────────────────────────────
-- 验证
-- ────────────────────────────────────────────────────────────
SELECT 'dimensions' AS tbl, name, join_on, LEFT(select_fields_json,60) AS sf
FROM vanna_semantic_dimensions
WHERE name IN ('category_dim','user_type_dim','city_dim','region_dim')
  AND (db_name = 'retail_dw' OR IFNULL(db_name,'') = '');

SELECT 'metrics' AS tbl, name, LEFT(extra_joins_json,80) AS extra_joins
FROM vanna_semantic_metrics
WHERE name IN ('vip_buyer_count','vip_gmv_rate','plus_gmv','net_revenue')
  AND (db_name = 'retail_dw' OR IFNULL(db_name,'') = '');
