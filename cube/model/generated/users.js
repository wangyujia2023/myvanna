cube(`users`, {
  sql_table: `retail_dw.dim_user_info`,
  title: `用户维表`,
  public: true,

  measures: {
  },

  dimensions: {
    is_plus_vip: {
      sql: `is_plus_vip`,
      type: `boolean`,
      title: `PLUS会员`,
    },
    user_id: {
      sql: `user_id`,
      type: `number`,
      title: `用户ID`,
      primary_key: true,
    },
  },

});
