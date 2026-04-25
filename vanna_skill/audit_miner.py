"""
audit_log SQL 挖掘器
- 从 Doris 审计日志中提取高质量 SELECT SQL
- 质量过滤：执行成功 + 时间合理 + 有结果 + 非系统查询
- 用 Gemini 反向生成对应的「问题」
- 批量写入 Vanna 知识库
"""
import logging
import re
from typing import Dict, List, Tuple

from .doris_client import DorisClient

logger = logging.getLogger(__name__)

# ── 质量过滤配置 ──────────────────────────────────────────────────────────────
QUALITY_RULES = [
    (lambda sql: len(sql.strip()) > 40,             "SQL 太短"),
    (lambda sql: sql.strip().upper().startswith("SELECT"), "非 SELECT"),
    (lambda sql: "information_schema" not in sql.lower(), "系统表查询"),
    (lambda sql: "vanna_store" not in sql.lower(),   "Vanna 系统查询"),
    (lambda sql: "audit_log" not in sql.lower(),     "audit_log 自查询"),
    (lambda sql: sql.count(" ") < 1000,              "SQL 过长"),
]

_AUDIT_QUERY = """
SELECT
    query_id,
    stmt,
    query_time,
    scan_bytes,
    return_rows,
    client_ip,
    `user`,
    db
FROM __internal_schema.audit_log
WHERE 1=1
  AND state      = 'EOF'
  AND query_time BETWEEN 10 AND {max_ms}
  AND scan_bytes < {max_bytes}
  AND return_rows BETWEEN 1 AND 500000
  AND LENGTH(stmt) > 40
ORDER BY query_time ASC
LIMIT {limit}
"""


def _passes_quality(sql: str) -> Tuple[bool, str]:
    """返回 (通过, 失败原因)"""
    for rule_fn, reason in QUALITY_RULES:
        try:
            if not rule_fn(sql):
                return False, reason
        except Exception:
            pass
    return True, ""


def _dedup(sql_list: List[str], similarity_threshold: int = 10) -> List[str]:
    """简单去重：基于 SQL 前 N 字符"""
    seen = set()
    result = []
    for sql in sql_list:
        key = re.sub(r"\s+", " ", sql.strip().lower())[:similarity_threshold * 5]
        if key not in seen:
            seen.add(key)
            result.append(sql)
    return result


def _normalize_sql(sql: str) -> str:
    return re.sub(r"\s+", " ", sql.strip().lower())


class AuditMiner:
    """
    从 audit_log 挖掘高质量 SQL 并入库
    """

    def __init__(self, biz_doris: DorisClient, vanna_instance=None):
        self._db = biz_doris
        self._vanna = vanna_instance   # DorisVanna 实例（用于 generate_question + add_question_sql）

    def mine(
        self,
        max_query_time_ms: int = 30_000,
        max_scan_bytes: int = 10 * 1024 ** 3,  # 10GB
        limit: int = 2000,
        auto_generate_question: bool = True,
    ) -> dict:
        """
        执行挖掘流程
        返回 {"total_raw": N, "passed_quality": N, "added": N, "skipped": N, "failed": N}
        """
        logger.info(f"[AuditMiner] 开始挖掘，limit={limit}")

        # 1. 从 audit_log 拉取
        try:
            rows = self._db.execute(
                _AUDIT_QUERY.format(
                    max_ms=max_query_time_ms,
                    max_bytes=max_scan_bytes,
                    limit=limit,
                )
            )
        except Exception as e:
            logger.error(f"[AuditMiner] 查询 audit_log 失败: {e}")
            return {"error": str(e)}

        raw_sqls = [r["stmt"] for r in rows if r.get("stmt")]
        total_raw = len(raw_sqls)
        logger.info(f"[AuditMiner] 拉取 {total_raw} 条原始 SQL")

        # 2. 质量过滤
        passed = []
        for sql in raw_sqls:
            ok, reason = _passes_quality(sql)
            if ok:
                passed.append(sql)
            else:
                logger.debug(f"[AuditMiner] 过滤: {reason}  SQL: {sql[:60]}")
        logger.info(f"[AuditMiner] 质量过滤后保留 {len(passed)} 条")

        # 3. 去重
        deduped = _dedup(passed)
        logger.info(f"[AuditMiner] 去重后 {len(deduped)} 条")

        # 4. 生成问题并批量入库
        prepared: List[Dict[str, str]] = []
        skipped, failed = 0, 0
        prepared_seen: set[tuple[str, str]] = set()
        for sql in deduped:
            try:
                question = ""
                if auto_generate_question and self._vanna:
                    question = self._vanna.generate_question(sql)

                if question and self._vanna:
                    key = (question.strip(), _normalize_sql(sql))
                    if key not in prepared_seen:
                        prepared_seen.add(key)
                        prepared.append({
                            "question": question.strip(),
                            "sql": sql.strip(),
                        })
                    else:
                        skipped += 1
                else:
                    skipped += 1
            except Exception as e:
                failed += 1
                logger.warning(f"[AuditMiner] 生成问题失败: {e}")

        added = 0
        if prepared and self._vanna:
            batch_result = self._vanna.add_question_sql_batch(
                prepared,
                source="audit_log",
            )
            added = batch_result["added"]
            skipped += batch_result["skipped"]

        result = {
            "total_raw": total_raw,
            "passed_quality": len(passed),
            "deduped": len(deduped),
            "prepared": len(prepared),
            "added": added,
            "skipped": skipped,
            "failed": failed,
        }
        logger.info(f"[AuditMiner] 完成: {result}")
        return result

    def preview(self, limit: int = 20) -> list:
        """预览可挖掘的 SQL 列表（不入库）"""
        try:
            rows = self._db.execute(
                _AUDIT_QUERY.format(max_ms=30000, max_bytes=10**10, limit=limit * 3)
            )
        except Exception as e:
            return [{"error": str(e)}]

        result = []
        for r in rows:
            sql = r.get("stmt", "")
            ok, reason = _passes_quality(sql)
            if ok:
                result.append({
                    "sql": sql[:200],
                    "query_time_ms": r.get("query_time", 0),
                    "return_rows": r.get("return_rows", 0),
                    "db": r.get("db", ""),
                })
                if len(result) >= limit:
                    break
        return result
