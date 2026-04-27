
CREATE DATABASE IF NOT EXISTS retail_dw;
USE retail_dw;

-- ==========================================================================
-- 1. DIM 层 (维度表 - 5张)
-- ==========================================================================

CREATE TABLE IF NOT EXISTS dim_user_info (
    user_id         BIGINT          COMMENT '用户全局唯一ID', 
    phone_hash      VARCHAR(64)     COMMENT '脱敏手机号', 
    gender          TINYINT         COMMENT '性别：0未知，1男，2女', 
    age_group       VARCHAR(16)     COMMENT '年龄段', 
    city_code       VARCHAR(32)     COMMENT '城市编码', 
    is_plus_vip     BOOLEAN         COMMENT '是否为PLUS会员', 
    register_time   DATETIME        COMMENT '注册时间'
) ENGINE=OLAP UNIQUE KEY(user_id) 
COMMENT '用户主数据维度表'
DISTRIBUTED BY HASH(user_id) BUCKETS 3 PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS dim_sku_info (
    sku_id          BIGINT          COMMENT '商品SKU唯一ID', 
    spu_id          BIGINT          COMMENT '商品SPU ID', 
    sku_name        VARCHAR(256)    COMMENT '商品展示名称', 
    category_1_id   INT             COMMENT '一级类目ID', 
    category_2_id   INT             COMMENT '二级类目ID', 
    category_3_id   INT             COMMENT '三级类目ID', 
    brand_id        INT             COMMENT '品牌ID', 
    retail_price    DECIMAL(12,2)   COMMENT '吊牌价'
) ENGINE=OLAP UNIQUE KEY(sku_id) 
COMMENT '商品主数据维度表'
DISTRIBUTED BY HASH(sku_id) BUCKETS 3 PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS dim_store_info (
    store_id        INT             COMMENT '门店或前置仓ID', 
    store_name      VARCHAR(128)    COMMENT '门店名称', 
    region_id       INT             COMMENT '大区ID', 
    store_type      VARCHAR(32)     COMMENT '经营类型：直营/加盟/前置仓'
) ENGINE=OLAP UNIQUE KEY(store_id) 
COMMENT '门店与前置仓维度表'
DISTRIBUTED BY HASH(store_id) BUCKETS 1 PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS dim_coupon_rule (
    coupon_id       BIGINT          COMMENT '优惠券规则ID', 
    coupon_name     VARCHAR(128)    COMMENT '优惠券名称', 
    discount_type   VARCHAR(32)     COMMENT '类型：满减/折扣/直降', 
    min_amount      DECIMAL(10,2)   COMMENT '门槛金额', 
    discount_amount DECIMAL(10,2)   COMMENT '抵扣金额或比例'
) ENGINE=OLAP UNIQUE KEY(coupon_id) 
COMMENT '营销优惠券规则维度表'
DISTRIBUTED BY HASH(coupon_id) BUCKETS 1 PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS dim_date (
    dt              DATE            COMMENT '标准日期', 
    year_month      VARCHAR(10)     COMMENT '年月', 
    day_of_week     TINYINT         COMMENT '周几(1-7)', 
    is_weekend      BOOLEAN         COMMENT '是否周末', 
    is_holiday      BOOLEAN         COMMENT '是否节假日'
) ENGINE=OLAP UNIQUE KEY(dt) 
COMMENT '日期维度表'
DISTRIBUTED BY HASH(dt) BUCKETS 1 PROPERTIES ("replication_num" = "1");

-- ==========================================================================
-- 2. ODS 层 (原始流水表 - 5张)
-- ==========================================================================

CREATE TABLE IF NOT EXISTS ods_trade_order (
    dt              DATE            COMMENT '分区日期', 
    order_id        BIGINT          COMMENT '交易订单ID', 
    user_id         BIGINT          COMMENT '下单用户ID', 
    store_id        INT             COMMENT '门店ID', 
    order_status    TINYINT         COMMENT '状态：10未付, 20已付, 40完成, 50取消', 
    pay_amount      DECIMAL(12,2)   COMMENT '应付金额', 
    create_time     DATETIME        COMMENT '创建时间'
) ENGINE=OLAP UNIQUE KEY(dt, order_id) 
COMMENT '交易订单主表'
DISTRIBUTED BY HASH(order_id) BUCKETS 3 PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS ods_trade_order_detail (
    dt              DATE            COMMENT '分区日期', 
    order_id        BIGINT          COMMENT '订单ID', 
    sku_id          BIGINT          COMMENT '商品SKU ID', 
    sku_num         INT             COMMENT '数量', 
    sku_price       DECIMAL(12,2)   COMMENT '单价', 
    apportion_amt   DECIMAL(12,2)   COMMENT '分摊金额'
) ENGINE=OLAP UNIQUE KEY(dt, order_id, sku_id) 
COMMENT '交易订单明细表'
DISTRIBUTED BY HASH(order_id) BUCKETS 3 PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS ods_payment_log (
    dt              DATE            COMMENT '分区日期', 
    pay_id          VARCHAR(64)     COMMENT '支付流水号', 
    order_id        BIGINT          COMMENT '订单ID', 
    pay_method      VARCHAR(32)     COMMENT '支付渠道', 
    pay_time        DATETIME        COMMENT '支付成功时间'
) ENGINE=OLAP DUPLICATE KEY(dt, pay_id,order_id) 
COMMENT '支付流水表'
DISTRIBUTED BY HASH(order_id) BUCKETS 3 PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS ods_traffic_page_view (
    dt              DATE            COMMENT '分区日期', 
    log_time        DATETIME        COMMENT '日志时间', 
    device_id       VARCHAR(128)    COMMENT '设备ID', 
    user_id         BIGINT          COMMENT '用户ID', 
    page_id         VARCHAR(64)     COMMENT '页面ID', 
    source_type     VARCHAR(32)     COMMENT '来源渠道'
) ENGINE=OLAP DUPLICATE KEY(dt, log_time, device_id) 
COMMENT '点击曝光日志表'
DISTRIBUTED BY HASH(device_id) BUCKETS 3 PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS ods_cart_info (
    dt              DATE            COMMENT '分区日期', 
    user_id         BIGINT          NOT NULL, 
    sku_id          BIGINT          NOT NULL, 
    sku_num         INT             COMMENT '件数', 
    add_time        DATETIME        COMMENT '加购时间'
) ENGINE=OLAP UNIQUE KEY(dt, user_id, sku_id) 
COMMENT '购物车状态表'
DISTRIBUTED BY HASH(user_id) BUCKETS 3 PROPERTIES ("replication_num" = "1");

-- ==========================================================================
-- 3. DWD 层 (明细事实/宽表 - 4张)
-- ==========================================================================

CREATE TABLE IF NOT EXISTS dwd_trade_order_wide (
    dt              DATE            COMMENT '业务日期', 
    order_id        BIGINT          COMMENT '订单ID', 
    sku_id          BIGINT          COMMENT '商品ID', 
    user_id         BIGINT          COMMENT '用户ID', 
    city_code       VARCHAR(32)     COMMENT '冗余城市', 
    store_id        INT             COMMENT '冗余门店', 
    category_1_id   INT             COMMENT '冗余类目', 
    sku_price       DECIMAL(12,2)   COMMENT '单价', 
    sku_num         INT             COMMENT '件数', 
    apportion_amt   DECIMAL(12,2)   COMMENT '分摊实付', 
    order_status    TINYINT         COMMENT '最新状态', 
    create_time     DATETIME        COMMENT '创建时间'
) ENGINE=OLAP UNIQUE KEY(dt, order_id, sku_id) 
COMMENT '交易订单大宽表'
DISTRIBUTED BY HASH(order_id) BUCKETS 3 PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS dwd_trade_refund_fact (
    dt              DATE            COMMENT '退款日期', 
    refund_id       BIGINT          COMMENT '退款ID', 
    order_id        BIGINT          COMMENT '单号', 
    sku_id          BIGINT          COMMENT '商品', 
    refund_amt      DECIMAL(12,2)   COMMENT '金额', 
    refund_reason   VARCHAR(128)    COMMENT '原因'
) ENGINE=OLAP UNIQUE KEY(dt, refund_id,order_id) 
COMMENT '退款事实明细表'
DISTRIBUTED BY HASH(order_id) BUCKETS 3 PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS dwd_marketing_coupon_use (
    dt              DATE            COMMENT '用券日期', 
    coupon_id       BIGINT          COMMENT '券ID', 
    user_id         BIGINT          COMMENT '用户', 
    order_id        BIGINT          COMMENT '订单', 
    use_time        DATETIME        COMMENT '核销时间'
) ENGINE=OLAP UNIQUE KEY(dt, coupon_id, user_id) 
COMMENT '优惠券核销事实表'
DISTRIBUTED BY HASH(user_id) BUCKETS 3 PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS dwd_traffic_session (
    dt              DATE            COMMENT '日期', 
    session_id      VARCHAR(128)    COMMENT '会话ID', 
    user_id         BIGINT          COMMENT '用户', 
    entry_page      VARCHAR(64)     COMMENT '入口页', 
    leave_page      VARCHAR(64)     COMMENT '出口页', 
    duration_sec    INT             COMMENT '时长(秒)'
) ENGINE=OLAP DUPLICATE KEY(dt, session_id) 
COMMENT '流量会话汇总明细表'
DISTRIBUTED BY HASH(session_id) BUCKETS 3 PROPERTIES ("replication_num" = "1");

-- ==========================================================================
-- 4. DWS 层 (轻度汇总 - 3张)
-- ==========================================================================

CREATE TABLE IF NOT EXISTS dws_user_trade_daily (
    dt              DATE            COMMENT '日期', 
    user_id         BIGINT          COMMENT '用户ID', 
    pay_order_cnt   BIGINT SUM DEFAULT "0" COMMENT '支付单数', 
    pay_sku_cnt     BIGINT SUM DEFAULT "0" COMMENT '购买件数', 
    pay_amount      DECIMAL(14,2) SUM DEFAULT "0.00" COMMENT '支付总额'
) ENGINE=OLAP AGGREGATE KEY(dt, user_id) 
COMMENT '用户交易日汇总'
DISTRIBUTED BY HASH(user_id) BUCKETS 3 PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS dws_sku_trade_daily (
    dt              DATE            COMMENT '日期', 
    sku_id          BIGINT          COMMENT '商品ID', 
    store_id        INT             COMMENT '门店ID', 
    sale_qty        BIGINT SUM DEFAULT "0" COMMENT '销量', 
    sale_amt        DECIMAL(14,2) SUM DEFAULT "0.00" COMMENT '销额'
) ENGINE=OLAP AGGREGATE KEY(dt, sku_id, store_id) 
COMMENT '商品门店交易日汇总'
DISTRIBUTED BY HASH(sku_id) BUCKETS 3 PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS dws_store_traffic_trade_daily (
    dt              DATE            COMMENT '日期', 
    store_id        INT             COMMENT '门店ID', 
    visit_uv        BIGINT SUM DEFAULT "0" COMMENT '访问访客数', 
    pay_order_cnt   BIGINT SUM DEFAULT "0" COMMENT '支付单数', 
    pay_amt         DECIMAL(14,2) SUM DEFAULT "0.00" COMMENT '支付营收'
) ENGINE=OLAP AGGREGATE KEY(dt, store_id) 
COMMENT '门店日度流量交易汇总'
DISTRIBUTED BY HASH(store_id) BUCKETS 1 PROPERTIES ("replication_num" = "1");

-- ==========================================================================
-- 5. ADS 层 (应用报表 - 3张)
-- ==========================================================================

CREATE TABLE IF NOT EXISTS ads_sales_cockpit (
    dt              DATE            COMMENT '日期', 
    total_gmv       DECIMAL(16,2)   COMMENT '全盘总营收', 
    total_orders    BIGINT          COMMENT '全盘总单量', 
    total_buyers    BIGINT          COMMENT '全盘下单人数', 
    gmv_wow_ratio   DECIMAL(8,4)    COMMENT 'GMV周环比率', 
    update_time     DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间'
) ENGINE=OLAP UNIQUE KEY(dt) 
COMMENT '核心销售大屏报表'
DISTRIBUTED BY HASH(dt) BUCKETS 1 PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS ads_user_funnel (
    dt              DATE            COMMENT '日期', 
    platform        VARCHAR(32)     COMMENT '端别', 
    home_uv         BIGINT          COMMENT '首页UV', 
    detail_uv       BIGINT          COMMENT '商详页UV', 
    cart_uv         BIGINT          COMMENT '加购UV', 
    pay_uv          BIGINT          COMMENT '支付UV', 
    home_to_pay_rate DECIMAL(8,4)   COMMENT '首页-支付总转化率'
) ENGINE=OLAP UNIQUE KEY(dt, platform) 
COMMENT '流量转化漏斗报表'
DISTRIBUTED BY HASH(dt) BUCKETS 1 PROPERTIES ("replication_num" = "1");

CREATE TABLE IF NOT EXISTS ads_rfm_segment (
    user_id         BIGINT          COMMENT '用户ID', 
    recency_days    INT             COMMENT '最近消费天数(R)', 
    frequency_cnt   INT             COMMENT '消费频次(F)', 
    monetary_amt    DECIMAL(14,2)   COMMENT '消费金额(M)', 
    user_segment    VARCHAR(32)     COMMENT '客群标签'
) ENGINE=OLAP UNIQUE KEY(user_id) 
COMMENT 'RFM价值分层模型'
DISTRIBUTED BY HASH(user_id) BUCKETS 3 PROPERTIES ("replication_num" = "1");


-- ==========================================================================
-- 6. MOCK 数据与 ETL 全链路示例 (以 2024-05-01 为例)
-- ==========================================================================

-- A. 基础维度注入
INSERT INTO dim_user_info VALUES (101, 'HASH_U001', 1, '18-25', 'BJ', true, '2023-01-01 10:00:00');
INSERT INTO dim_user_info VALUES (102, 'HASH_U002', 2, '26-35', 'SH', false, '2023-05-15 14:30:00');

INSERT INTO dim_sku_info VALUES (2001, 901, 'iPhone 15', 10, 101, 1011, 1, 5999.00);
INSERT INTO dim_sku_info VALUES (2002, 902, '三只松鼠', 20, 201, 2011, 2, 99.00);

-- B. 原始流水注入 (ODS)
INSERT INTO ods_trade_order VALUES ('2024-05-01', 8801, 101, 1, 20, 6197.00, '2024-05-01 10:15:00');
INSERT INTO ods_trade_order VALUES ('2024-05-01', 8802, 102, 2, 20, 5999.00, '2024-05-01 11:30:00');

INSERT INTO ods_trade_order_detail VALUES ('2024-05-01', 8801, 2001, 1, 5999.00, 5999.00);
INSERT INTO ods_trade_order_detail VALUES ('2024-05-01', 8801, 2002, 2, 99.00, 198.00);
INSERT INTO ods_trade_order_detail VALUES ('2024-05-01', 8802, 2001, 1, 5999.00, 5999.00);

-- C. ETL: ODS -> DWD (打宽清洗)
INSERT INTO dwd_trade_order_wide
SELECT 
    od.dt, od.order_id, odd.sku_id, od.user_id, u.city_code, od.store_id, 
    s.category_1_id, odd.sku_price, odd.sku_num, odd.apportion_amt, 
    od.order_status, od.create_time
FROM ods_trade_order od
JOIN ods_trade_order_detail odd ON od.order_id = odd.order_id AND od.dt = odd.dt
LEFT JOIN dim_user_info u ON od.user_id = u.user_id
LEFT JOIN dim_sku_info s ON odd.sku_id = s.sku_id
WHERE od.dt = '2024-05-01';

-- D. ETL: DWD -> DWS (日粒度聚合)
INSERT INTO dws_user_trade_daily
SELECT dt, user_id, COUNT(DISTINCT order_id), SUM(sku_num), SUM(apportion_amt)
FROM dwd_trade_order_wide WHERE dt = '2024-05-01' GROUP BY dt, user_id;

-- E. ETL: DWS -> ADS (报表产出)
INSERT INTO ads_sales_cockpit (dt, total_gmv, total_orders, total_buyers)
SELECT dt, SUM(pay_amount), SUM(pay_order_cnt), COUNT(DISTINCT user_id)
FROM dws_user_trade_daily WHERE dt = '2024-05-01' GROUP BY dt;

-- ==========================================================================
-- 验证：SELECT * FROM ads_sales_cockpit WHERE dt = '2024-05-01';
-- ==========================================================================