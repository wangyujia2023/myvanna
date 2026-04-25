import pymysql
import pandas as pd

# 我们不再继承，而是创建一个简单的管理类
class DorisManager:
    def __init__(self, config):
        self.config = config

    def run_sql(self, sql: str):
        # 使用 pymysql 连接执行
        conn = pymysql.connect(**self.config)
        return pd.read_sql(sql, conn)

# 1. 配置 Doris 信息
doris_config = {
    'host': '10.26.20.3',
    'port': 19030,
    'user': 'root',
    'password': '',
    'database': 'retail_dw'  # 请确保该库存在
}

# 2. 验证连接
try:
    manager = DorisManager(doris_config)
    print("Doris 连接配置已初始化。")
    
    # 尝试查询
    df = manager.run_sql("SELECT 1")
    print("连接成功！查询结果：")
    print(df)
except Exception as e:
    print(f"连接失败: {e}")