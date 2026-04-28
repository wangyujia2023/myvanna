cube(`cities`, {
  sql_table: `retail_dw.dim_city_info`,
  title: `城市维表`,
  public: true,

  measures: {
  },

  dimensions: {
    city_code: {
      sql: `city_code`,
      type: `string`,
      title: `城市编码`,
      primary_key: true,
    },
    city_name: {
      sql: `city_name`,
      type: `string`,
      title: `城市名称`,
    },
    region_name: {
      sql: `region_name`,
      type: `string`,
      title: `大区名称`,
    },
  },

});
