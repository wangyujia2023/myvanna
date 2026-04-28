cube(`member_types`, {
  sql_table: `retail_dw.dim_member_type_info`,
  title: `会员类型维表`,
  public: true,

  measures: {
  },

  dimensions: {
    member_level: {
      sql: `member_level`,
      type: `string`,
      title: `会员等级`,
    },
    member_type: {
      sql: `member_type_name`,
      type: `string`,
      title: `会员类型`,
    },
    member_type_code: {
      sql: `member_type_code`,
      type: `number`,
      title: `会员类型编码`,
      primary_key: true,
    },
  },

});
