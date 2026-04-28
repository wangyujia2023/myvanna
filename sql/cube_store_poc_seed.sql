USE cube_store;

INSERT INTO cube_models (model_id, cube_name, title, sql_table, data_source, public_flag, visible, version)
VALUES
  (1, 'orders', '订单宽表', 'retail_dw.dwd_trade_order_wide', 'default', 1, 1, 1),
  (2, 'users', '用户维表', 'retail_dw.dim_user_info', 'default', 1, 1, 1),
  (3, 'stores', '门店维表', 'retail_dw.dim_store_info', 'default', 1, 1, 1),
  (4, 'refunds', '退款事实表', 'retail_dw.dwd_trade_refund_fact', 'default', 1, 1, 1),
  (5, 'cities', '城市维表', 'retail_dw.dim_city_info', 'default', 1, 1, 1),
  (6, 'member_types', '会员类型维表', 'retail_dw.dim_member_type_info', 'default', 1, 1, 1);

INSERT INTO cube_measures (measure_id, cube_name, measure_name, title, sql_expr, measure_type, format, visible, version)
VALUES
  (101, 'orders', 'gmv', '销售额', 'apportion_amt', 'sum', 'currency', 1, 1),
  (102, 'orders', 'consume_amt', '消费金额', 'apportion_amt', 'sum', 'currency', 1, 1),
  (103, 'orders', 'plus_consume_amt', 'PLUS会员消费金额', 'CASE WHEN {users.is_plus_vip} = 1 THEN {CUBE}.apportion_amt ELSE 0 END', 'sum', 'currency', 1, 1),
  (104, 'orders', 'buyer_count', '买家数', 'user_id', 'countDistinct', 'number', 1, 1),
  (105, 'orders', 'aov', '客单价', 'apportion_amt', 'avg', 'currency', 1, 1),
  (106, 'orders', 'net_revenue', '净收入', 'SUM({CUBE}.apportion_amt) - COALESCE(SUM({refunds.refund_amt}), 0)', 'number', 'currency', 1, 1),
  (107, 'orders', 'order_count', '订单量', 'order_id', 'countDistinct', 'number', 1, 1);

INSERT INTO cube_dimensions (dimension_id, cube_name, dimension_name, title, sql_expr, dimension_type, primary_key_flag, enum_mapping_json, visible, version)
VALUES
  (201, 'orders', 'order_id', '订单ID', 'order_id', 'number', 1, NULL, 1, 1),
  (202, 'orders', 'city_code', '城市编码', 'city_code', 'string', 0, NULL, 0, 1),
  (203, 'orders', 'category_1_id', '一级类目', 'category_1_id', 'number', 0, NULL, 1, 1),
  (204, 'orders', 'store_id', '门店ID', 'store_id', 'number', 0, NULL, 1, 1),
  (205, 'orders', 'user_id', '用户ID', 'user_id', 'number', 0, NULL, 1, 1),
  (206, 'orders', 'dt', '业务日期', 'dt', 'time', 0, NULL, 1, 1),
  (212, 'orders', 'time_day', '日期', 'dt', 'time', 0, NULL, 1, 1),
  (213, 'orders', 'time_month', '月份', "DATE_FORMAT(dt, '%Y-%m')", 'string', 0, NULL, 1, 1),
  (207, 'users', 'user_id', '用户ID', 'user_id', 'number', 1, NULL, 1, 1),
  (208, 'users', 'city_code', '城市编码', 'city_code', 'string', 0, NULL, 0, 1),
  (209, 'users', 'is_plus_vip', 'PLUS会员', 'is_plus_vip', 'boolean', 0, NULL, 1, 1),
  (214, 'users', 'member_type_raw', '会员类型(用户表计算)', "CASE WHEN {CUBE}.is_plus_vip = 1 THEN 'PLUS会员' ELSE '普通会员' END", 'string', 0, NULL, 0, 1),
  (210, 'stores', 'store_id', '门店ID', 'store_id', 'number', 1, NULL, 1, 1),
  (211, 'stores', 'store_type', '门店类型', 'store_type', 'string', 0, NULL, 1, 1),
  (222, 'stores', 'store_name', '门店名称', 'store_name', 'string', 0, NULL, 1, 1),
  (215, 'refunds', 'order_id', '订单ID', 'order_id', 'number', 1, NULL, 1, 1),
  (216, 'cities', 'city_name', '城市名称', 'city_name', 'string', 0, NULL, 1, 1),
  (217, 'cities', 'region_name', '大区名称', 'region_name', 'string', 0, NULL, 1, 1),
  (218, 'cities', 'city_code', '城市编码', 'city_code', 'string', 1, NULL, 1, 1),
  (219, 'member_types', 'member_type', '会员类型', 'member_type_name', 'string', 0, NULL, 1, 1),
  (220, 'member_types', 'member_type_code', '会员类型编码', 'member_type_code', 'number', 1, NULL, 1, 1),
  (221, 'member_types', 'member_level', '会员等级', 'member_level', 'string', 0, NULL, 1, 1);

INSERT INTO cube_dimension_values
  (value_id, cube_name, dimension_name, value_code, value_label, aliases_json, source, source_table, source_column, usage_count, visible, version)
VALUES
  (505, 'member_types', 'member_type', '1', 'PLUS会员', '["PLUS 会员","plus会员","付费会员","高价值会员"]', 'seed', 'retail_dw.dim_member_type_info', 'member_type_name', 0, 1, 1),
  (506, 'member_types', 'member_type', '0', '普通会员', '["非PLUS会员","非 PLUS 会员","普通用户","非会员"]', 'seed', 'retail_dw.dim_member_type_info', 'member_type_name', 0, 1, 1);

INSERT INTO cube_joins (join_id, cube_name, target_cube, relationship, join_type, join_sql, visible, version)
VALUES
  (301, 'orders', 'users', 'many_to_one', 'LEFT', '{CUBE}.user_id = {users}.user_id', 1, 1),
  (302, 'orders', 'stores', 'many_to_one', 'LEFT', '{CUBE}.store_id = {stores}.store_id', 1, 1),
  (303, 'orders', 'refunds', 'one_to_many', 'LEFT', '{CUBE}.order_id = {refunds}.order_id', 1, 1),
  (304, 'orders', 'cities', 'many_to_one', 'LEFT', '{CUBE}.city_code = {cities}.city_code', 1, 1),
  (305, 'orders', 'member_types', 'many_to_one', 'LEFT', '{users.is_plus_vip} = {member_types}.member_type_code', 1, 1);

INSERT INTO cube_segments (segment_id, cube_name, segment_name, title, filter_sql, visible, version)
VALUES
  (401, 'orders', 'plus_members', 'PLUS会员', '{users.is_plus_vip} = 1', 1, 1),
  (402, 'orders', 'normal_members', '普通会员', '{users.is_plus_vip} = 0', 1, 1);

INSERT INTO cube_sql_templates
  (template_id, template_name, template_type, title, template_sql, params_json, visible, version)
VALUES
  (
    601,
    'topn_per_group',
    'window',
    '分组内 TopN',
    'WITH base AS (
{base_sql}
),
ranked AS (
SELECT
  base.*,
  ROW_NUMBER() OVER (PARTITION BY {partition_by} ORDER BY {order_by} {direction}) AS rn
FROM base
)
SELECT
  {select_columns}
FROM ranked
WHERE rn <= {top_n}
ORDER BY {partition_by} ASC, rn ASC',
    '{"base_sql":"Cube 聚合 SQL","partition_by":"分区维度，如 city_code","order_by":"排序指标，如 gmv","direction":"ASC/DESC","top_n":"每组保留 N 条","select_columns":"外层输出列"}',
    1,
    1
  );

INSERT INTO cube_semantic_aliases
  (alias_id, entity_type, entity_name, alias_text, source, match_type, weight, visible, version)
VALUES
  (7001, 'measure', 'gmv', 'GMV', 'seed', 'contains', 1.0, 1, 1),
  (7002, 'measure', 'gmv', '销售额', 'seed', 'contains', 1.0, 1, 1),
  (7003, 'measure', 'gmv', '销售总额', 'seed', 'contains', 1.0, 1, 1),
  (7004, 'measure', 'gmv', '总销售额', 'seed', 'contains', 1.0, 1, 1),
  (7005, 'measure', 'gmv', '收入', 'seed', 'contains', 0.8, 1, 1),
  (7013, 'measure', 'gmv', '消费金额', 'seed', 'contains', 1.0, 1, 1),
  (7014, 'measure', 'gmv', '花了多少钱', 'seed', 'contains', 1.0, 1, 1),
  (7015, 'measure', 'gmv', '总消费', 'seed', 'contains', 1.0, 1, 1),
  (7019, 'measure', 'gmv', '消费', 'seed', 'contains', 0.9, 1, 1),
  (7016, 'measure', 'gmv', '总金额', 'seed', 'contains', 0.9, 1, 1),
  (7017, 'measure', 'gmv', '交易额', 'seed', 'contains', 0.9, 1, 1),
  (7018, 'measure', 'gmv', '营业额', 'seed', 'contains', 0.9, 1, 1),
  (7006, 'measure', 'order_count', '订单量', 'seed', 'contains', 1.0, 1, 1),
  (7007, 'measure', 'order_count', '订单数', 'seed', 'contains', 1.0, 1, 1),
  (7008, 'measure', 'buyer_count', '下单人数', 'seed', 'contains', 1.0, 1, 1),
  (7009, 'measure', 'buyer_count', '买家数', 'seed', 'contains', 1.0, 1, 1),
  (7010, 'measure', 'aov', '客单价', 'seed', 'contains', 1.0, 1, 1),
  (7011, 'measure', 'net_revenue', '净收入', 'seed', 'contains', 1.0, 1, 1),
  (7012, 'measure', 'plus_consume_amt', 'PLUS会员消费', 'seed', 'contains', 1.0, 1, 1),
  (7101, 'dimension', 'city_name', '城市', 'seed', 'contains', 1.0, 1, 1),
  (7102, 'dimension', 'city_name', '地区', 'seed', 'contains', 1.0, 1, 1),
  (7108, 'dimension', 'city_code', '城市编码', 'seed', 'contains', 1.0, 1, 1),
  (7109, 'dimension', 'region_name', '大区', 'seed', 'contains', 1.0, 1, 1),
  (7110, 'dimension', 'region_name', '区域', 'seed', 'contains', 0.9, 1, 1),
  (7103, 'dimension', 'store_id', '门店', 'seed', 'contains', 1.0, 1, 1),
  (7104, 'dimension', 'store_type', '门店类型', 'seed', 'contains', 1.0, 1, 1),
  (7113, 'dimension', 'store_name', '门店名称', 'seed', 'contains', 1.0, 1, 1),
  (7105, 'dimension', 'member_type', '会员类型', 'seed', 'contains', 1.0, 1, 1),
  (7106, 'dimension', 'member_type', '会员', 'seed', 'contains', 0.8, 1, 1),
  (7112, 'dimension', 'member_level', '会员等级', 'seed', 'contains', 0.9, 1, 1),
  (7107, 'dimension', 'category_1_id', '一级类目', 'seed', 'contains', 1.0, 1, 1),
  (7111, 'dimension', 'category_1_id', '品类', 'seed', 'contains', 0.9, 1, 1),
  (7201, 'segment', 'plus_members', 'PLUS会员', 'seed', 'contains', 1.0, 1, 1),
  (7202, 'segment', 'normal_members', '普通会员', 'seed', 'contains', 1.0, 1, 1),
  (7301, 'template', 'topn_per_group', '排名', 'seed', 'contains', 1.0, 1, 1),
  (7302, 'template', 'topn_per_group', '排行', 'seed', 'contains', 1.0, 1, 1),
  (7303, 'template', 'topn_per_group', 'Top', 'seed', 'contains', 1.0, 1, 1);

INSERT INTO cube_model_versions (version_id, version_no, checksum, status, remark)
VALUES
  (1, 1, 'seed-v1', 'active', 'PoC 初始化版本');
