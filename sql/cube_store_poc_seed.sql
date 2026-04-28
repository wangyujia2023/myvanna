USE cube_store;

INSERT INTO cube_models (model_id, cube_name, title, sql_table, data_source, public_flag, visible, version)
VALUES
  (1, 'orders', '订单宽表', 'retail_dw.dwd_trade_order_wide', 'default', 1, 1, 1),
  (2, 'users', '用户维表', 'retail_dw.dim_user_info', 'default', 1, 1, 1),
  (3, 'stores', '门店维表', 'retail_dw.dim_store_info', 'default', 1, 1, 1),
  (4, 'refunds', '退款事实表', 'retail_dw.dwd_trade_refund_fact', 'default', 1, 1, 1);

INSERT INTO cube_measures (measure_id, cube_name, measure_name, title, sql_expr, measure_type, format, visible, version)
VALUES
  (101, 'orders', 'gmv', '销售额', 'apportion_amt', 'sum', 'currency', 1, 1),
  (102, 'orders', 'consume_amt', '消费金额', 'apportion_amt', 'sum', 'currency', 1, 1),
  (103, 'orders', 'plus_consume_amt', 'PLUS会员消费金额', 'CASE WHEN {users.is_plus_vip} = 1 THEN {CUBE}.apportion_amt ELSE 0 END', 'sum', 'currency', 1, 1),
  (104, 'orders', 'buyer_count', '买家数', 'user_id', 'countDistinct', 'number', 1, 1),
  (105, 'orders', 'aov', '客单价', 'apportion_amt', 'avg', 'currency', 1, 1),
  (106, 'orders', 'net_revenue', '净收入', 'SUM({CUBE}.apportion_amt) - COALESCE(SUM({refunds.refund_amt}), 0)', 'number', 'currency', 1, 1);

INSERT INTO cube_dimensions (dimension_id, cube_name, dimension_name, title, sql_expr, dimension_type, primary_key_flag, enum_mapping_json, visible, version)
VALUES
  (201, 'orders', 'order_id', '订单ID', 'order_id', 'number', 1, NULL, 1, 1),
  (202, 'orders', 'city_code', '城市编码', 'city_code', 'string', 0, '{"北京":"BJ","上海":"SH","广州":"GZ","深圳":"SZ"}', 1, 1),
  (203, 'orders', 'category_1_id', '一级类目', 'category_1_id', 'number', 0, NULL, 1, 1),
  (204, 'orders', 'store_id', '门店ID', 'store_id', 'number', 0, NULL, 1, 1),
  (205, 'orders', 'user_id', '用户ID', 'user_id', 'number', 0, NULL, 1, 1),
  (206, 'orders', 'dt', '业务日期', 'dt', 'time', 0, NULL, 1, 1),
  (212, 'orders', 'time_day', '日期', 'dt', 'time', 0, NULL, 1, 1),
  (213, 'orders', 'time_month', '月份', "DATE_FORMAT(dt, '%Y-%m')", 'string', 0, NULL, 1, 1),
  (207, 'users', 'user_id', '用户ID', 'user_id', 'number', 1, NULL, 1, 1),
  (208, 'users', 'city_code', '城市编码', 'city_code', 'string', 0, '{"北京":"BJ","上海":"SH","广州":"GZ","深圳":"SZ"}', 1, 1),
  (209, 'users', 'is_plus_vip', 'PLUS会员', 'is_plus_vip', 'boolean', 0, '{"PLUS会员":1,"普通会员":0}', 1, 1),
  (214, 'users', 'member_type', '会员类型', "CASE WHEN is_plus_vip = 1 THEN 'PLUS会员' ELSE '普通会员' END", 'string', 0, NULL, 1, 1),
  (210, 'stores', 'store_id', '门店ID', 'store_id', 'number', 1, NULL, 1, 1),
  (211, 'stores', 'store_type', '门店类型', 'store_type', 'string', 0, NULL, 1, 1),
  (215, 'refunds', 'order_id', '订单ID', 'order_id', 'number', 1, NULL, 1, 1);

INSERT INTO cube_dimension_values
  (value_id, cube_name, dimension_name, value_code, value_label, aliases_json, source, source_table, source_column, usage_count, visible, version)
VALUES
  (501, 'orders', 'city_code', 'BJ', '北京', '["北京市","北京地区"]', 'seed', 'retail_dw.dwd_trade_order_wide', 'city_code', 0, 1, 1),
  (502, 'orders', 'city_code', 'SH', '上海', '["上海市","上海地区"]', 'seed', 'retail_dw.dwd_trade_order_wide', 'city_code', 0, 1, 1),
  (503, 'orders', 'city_code', 'GZ', '广州', '["广州市","广州地区"]', 'seed', 'retail_dw.dwd_trade_order_wide', 'city_code', 0, 1, 1),
  (504, 'orders', 'city_code', 'SZ', '深圳', '["深圳市","深圳地区"]', 'seed', 'retail_dw.dwd_trade_order_wide', 'city_code', 0, 1, 1),
  (505, 'users', 'is_plus_vip', '1', 'PLUS会员', '["PLUS 会员","plus会员","会员"]', 'seed', 'retail_dw.dim_user_info', 'is_plus_vip', 0, 1, 1),
  (506, 'users', 'is_plus_vip', '0', '普通会员', '["非PLUS会员","非 PLUS 会员"]', 'seed', 'retail_dw.dim_user_info', 'is_plus_vip', 0, 1, 1),
  (507, 'users', 'member_type', 'PLUS会员', 'PLUS会员', '["PLUS 会员","plus会员"]', 'seed', 'retail_dw.dim_user_info', 'is_plus_vip', 0, 1, 1),
  (508, 'users', 'member_type', '普通会员', '普通会员', '["非PLUS会员","非 PLUS 会员"]', 'seed', 'retail_dw.dim_user_info', 'is_plus_vip', 0, 1, 1);

INSERT INTO cube_joins (join_id, cube_name, target_cube, relationship, join_type, join_sql, visible, version)
VALUES
  (301, 'orders', 'users', 'many_to_one', 'LEFT', '{CUBE}.user_id = {users}.user_id', 1, 1),
  (302, 'orders', 'stores', 'many_to_one', 'LEFT', '{CUBE}.store_id = {stores}.store_id', 1, 1),
  (303, 'orders', 'refunds', 'one_to_many', 'LEFT', '{CUBE}.order_id = {refunds}.order_id', 1, 1);

INSERT INTO cube_segments (segment_id, cube_name, segment_name, title, filter_sql, visible, version)
VALUES
  (401, 'orders', 'plus_members', 'PLUS会员', '{users.is_plus_vip} = 1', 1, 1),
  (402, 'orders', 'normal_members', '普通会员', '{users.is_plus_vip} = 0', 1, 1);

INSERT INTO cube_model_versions (version_id, version_no, checksum, status, remark)
VALUES
  (1, 1, 'seed-v1', 'active', 'PoC 初始化版本');
