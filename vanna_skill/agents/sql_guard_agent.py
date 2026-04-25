"""
SQL 防线 agent：危险词过滤 + Doris EXPLAIN 校验。
"""
from __future__ import annotations

from typing import Dict

import sqlparse

from .base import BaseAgent
from ..doris_client import DorisClient


class SQLGuardAgent(BaseAgent):
    name = "sql_guard_agent"

    _FORBIDDEN = ("drop", "alter", "truncate", "delete", "update", "insert")

    def __init__(self, biz_client: DorisClient):
        self._biz = biz_client

    def run(self, sql: str) -> Dict[str, object]:
        normalized = sqlparse.format(sql, keyword_case="lower", strip_comments=True)
        for token in self._FORBIDDEN:
            if token in normalized:
                return {"ok": False, "reason": f"forbidden_sql:{token}"}

        try:
            self._biz.execute(f"EXPLAIN {sql}")
            return {"ok": True, "reason": "explain_ok"}
        except Exception as e:
            return {"ok": False, "reason": str(e)}
