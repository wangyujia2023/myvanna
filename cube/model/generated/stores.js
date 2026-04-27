cube(`stores`, {
  sql_table: `retail_dw.dim_store_info`,
  title: `门店维表`,
  public: true,

  measures: {
  },

  dimensions: {
    store_id: {
      sql: `store_id`,
      type: `number`,
      title: `门店ID`,
      primary_key: true,
    },
    store_type: {
      sql: `store_type`,
      type: `string`,
      title: `门店类型`,
    },
  },

});
