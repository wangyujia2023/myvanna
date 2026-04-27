cube(`users`, {
  sql_table: `retail_dw.dim_user_info`,
  title: `用户维表`,
  public: true,

  measures: {
  },

  dimensions: {
    city_code: {
      sql: `city_code`,
      type: `string`,
      title: `城市编码`,
    },
    is_plus_vip: {
      sql: `is_plus_vip`,
      type: `boolean`,
      title: `PLUS会员`,
    },
    member_type: {
      sql: `CASE WHEN is_plus_vip = 1 THEN 'PLUS会员' ELSE '普通会员' END`,
      type: `string`,
      title: `会员类型`,
    },
    user_id: {
      sql: `user_id`,
      type: `number`,
      title: `用户ID`,
      primary_key: true,
    },
  },

});
