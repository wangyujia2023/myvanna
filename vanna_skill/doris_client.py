"""
Doris 连接管理 - 简单连接池 + 自动重连
"""
import logging
import threading
from typing import Optional

import pandas as pd
import pymysql
from pymysql.cursors import DictCursor

logger = logging.getLogger(__name__)


class DorisClient:
    """
    线程安全的 Doris MySQL 连接封装
    - 自动重连（连接断开时）
    - execute() 返回 dict list
    - query_df() 返回 DataFrame
    """

    def __init__(self, host: str, port: int, user: str, password: str,
                 database: str = "", connect_timeout: int = 10):
        self._cfg = dict(
            host=host, port=port, user=user, password=password,
            db=database, charset="utf8mb4",
            connect_timeout=connect_timeout,
            autocommit=True,
        )
        self._conn: Optional[pymysql.Connection] = None
        self._lock = threading.Lock()
        self._connect()

    def _connect(self):
        try:
            self._conn = pymysql.connect(**self._cfg)
            logger.info(f"[Doris] 连接成功 {self._cfg['host']}:{self._cfg['port']}"
                        f" db={self._cfg['db']}")
        except Exception as e:
            logger.error(f"[Doris] 连接失败: {e}")
            raise

    def _ensure_connected(self):
        try:
            self._conn.ping(reconnect=True)
        except Exception:
            logger.warning("[Doris] 连接丢失，正在重连...")
            self._connect()

    def execute(self, sql: str, args=None) -> list[dict]:
        """执行查询，返回 dict 列表"""
        with self._lock:
            self._ensure_connected()
            with self._conn.cursor(DictCursor) as cur:
                cur.execute(sql, args)
                return cur.fetchall()

    def execute_write(self, sql: str, args=None) -> int:
        """执行写操作，返回 affected rows"""
        with self._lock:
            self._ensure_connected()
            with self._conn.cursor() as cur:
                affected = cur.execute(sql, args)
                self._conn.commit()
                return affected

    def query_df(self, sql: str, args=None) -> pd.DataFrame:
        """执行查询，返回 DataFrame（直接用 cursor 转换，避免 pd.read_sql 与 pymysql 的兼容问题）"""
        rows = self.execute(sql, args)
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows)

    def use_db(self, db_name: str):
        """切换数据库"""
        self.execute_write(f"USE `{db_name}`")
        self._cfg["db"] = db_name

    def close(self):
        if self._conn:
            self._conn.close()

    def test(self) -> bool:
        """连通性测试"""
        try:
            result = self.execute("SELECT 1 AS ok")
            return result[0]["ok"] == 1
        except Exception as e:
            logger.error(f"[Doris] 连通性测试失败: {e}")
            return False
