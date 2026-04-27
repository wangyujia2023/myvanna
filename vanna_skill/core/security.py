from __future__ import annotations

import re


READ_ONLY_SQL_PATTERN = re.compile(r"^\s*(WITH\b|SELECT\b)", re.IGNORECASE)
DANGEROUS_SQL_PATTERN = re.compile(
    r"(?<![A-Za-z_])("
    r"DELETE|UPDATE|INSERT|UPSERT|REPLACE|MERGE|DROP|ALTER|TRUNCATE|CREATE|RENAME|"
    r"GRANT|REVOKE|SET|CALL|EXEC|LOAD|EXPORT|BACKUP|RESTORE|KILL"
    r")(?![A-Za-z_])",
    re.IGNORECASE,
)
DESTRUCTIVE_INTENT_PATTERN = re.compile(
    r"(?:"
    r"删(?:除|掉)?(?:.*?)(?:库|表|数据|字段|列|行|记录)?"
    r"|清空|清除(?:.*?数据)?|改表|改字段|加字段|删字段"
    r"|建表|建库|新建表|修改表|修改字段|截断|丢弃"
    r"|"
    r"(?<![a-zA-Z_])(?:drop|alter|truncate|delete|update|insert"
    r"|create\s+table|grant|revoke|replace\s+into)(?![a-zA-Z_])"
    r")",
    re.IGNORECASE,
)

DESTRUCTIVE_INTENT_MESSAGE = (
    "请求包含写操作或破坏性指令，系统仅支持只读查询。"
    "不能执行删除、修改、插入、建表、授权等操作。"
)


def normalize_sql_for_guard(sql: str) -> list[str]:
    """Remove comments and split SQL into non-empty statements for safety checks."""
    cleaned = (sql or "").strip()
    if not cleaned:
        raise ValueError("SQL 不能为空")
    normalized = re.sub(r"/\*.*?\*/", " ", cleaned, flags=re.DOTALL)
    normalized = re.sub(r"--[^\n]*", " ", normalized)
    return [part.strip() for part in normalized.split(";") if part.strip()]


def assert_readonly_sql(sql: str) -> None:
    statements = normalize_sql_for_guard(sql)
    if len(statements) != 1:
        raise ValueError("只允许单条只读查询")
    statement = statements[0]
    if not READ_ONLY_SQL_PATTERN.match(statement):
        raise ValueError("只允许只读查询（SELECT / WITH ... SELECT）")
    match = DANGEROUS_SQL_PATTERN.search(statement)
    if match:
        raise ValueError(f"SQL 安全拦截：不允许 {match.group(1).upper()} 操作")


def assert_safe_user_request(text: str) -> None:
    question = (text or "").strip()
    if not question:
        raise ValueError("问题不能为空")
    if DESTRUCTIVE_INTENT_PATTERN.search(question):
        raise ValueError(DESTRUCTIVE_INTENT_MESSAGE)
