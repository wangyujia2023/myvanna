cube(`refunds`, {
  sql_table: `retail_dw.dwd_trade_refund_fact`,
  title: `退款事实表`,
  public: true,

  measures: {
  },

  dimensions: {
    order_id: {
      sql: `order_id`,
      type: `number`,
      title: `订单ID`,
      primary_key: true,
    },
  },

});
