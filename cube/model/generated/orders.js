cube(`orders`, {
  sql_table: `retail_dw.dwd_trade_order_wide`,
  title: `订单宽表`,
  public: true,

  joins: {
    refunds: {
      sql: `{CUBE}.order_id = {refunds}.order_id`,
      relationship: `one_to_many`
    },
    stores: {
      sql: `{CUBE}.store_id = {stores}.store_id`,
      relationship: `many_to_one`
    },
    users: {
      sql: `{CUBE}.user_id = {users}.user_id`,
      relationship: `many_to_one`
    },
  },

  measures: {
    aov: {
      sql: `apportion_amt`,
      type: `avg`,
      title: `客单价`,
      format: `currency`,
    },
    buyer_count: {
      sql: `user_id`,
      type: `countDistinct`,
      title: `买家数`,
      format: `number`,
    },
    consume_amt: {
      sql: `apportion_amt`,
      type: `sum`,
      title: `消费金额`,
      format: `currency`,
    },
    gmv: {
      sql: `apportion_amt`,
      type: `sum`,
      title: `销售额`,
      format: `currency`,
    },
    net_revenue: {
      sql: `SUM({CUBE}.apportion_amt) - COALESCE(SUM({refunds.refund_amt}), 0)`,
      type: `number`,
      title: `净收入`,
      format: `currency`,
    },
    plus_consume_amt: {
      sql: `CASE WHEN {users.is_plus_vip} = 1 THEN {CUBE}.apportion_amt ELSE 0 END`,
      type: `sum`,
      title: `PLUS会员消费金额`,
      format: `currency`,
    },
  },

  dimensions: {
    category_1_id: {
      sql: `category_1_id`,
      type: `number`,
      title: `一级类目`,
    },
    city_code: {
      sql: `city_code`,
      type: `string`,
      title: `城市编码`,
    },
    dt: {
      sql: `dt`,
      type: `time`,
      title: `业务日期`,
    },
    order_id: {
      sql: `order_id`,
      type: `number`,
      title: `订单ID`,
      primary_key: true,
    },
    store_id: {
      sql: `store_id`,
      type: `number`,
      title: `门店ID`,
    },
    time_day: {
      sql: `dt`,
      type: `time`,
      title: `日期`,
    },
    time_month: {
      sql: `DATE_FORMAT(dt, '%Y-%m')`,
      type: `string`,
      title: `月份`,
    },
    user_id: {
      sql: `user_id`,
      type: `number`,
      title: `用户ID`,
    },
  },

  segments: {
    normal_members: {
      sql: `{users.is_plus_vip} = 0`
    },
    plus_members: {
      sql: `{users.is_plus_vip} = 1`
    },
  },
});
