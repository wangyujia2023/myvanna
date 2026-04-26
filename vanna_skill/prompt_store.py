"""
Prompt Lab 数据库存储。

容错策略：
- DB 表不存在或连接失败时，所有读操作返回 fallback 状态（基于 default_prompt 参数）
- 写操作抛出原始异常，由 API 层决定如何响应
"""
from __future__ import annotations

import logging
from typing import Dict, List

from .doris_client import DorisClient

logger = logging.getLogger(__name__)


class PromptStore:
    def __init__(self, doris: DorisClient):
        self._db = doris

    # ─────────────────────────────────────────────────────────────────────────
    # 内部工具
    # ─────────────────────────────────────────────────────────────────────────
    def _set_config(self, key: str, value: str):
        """Doris UNIQUE KEY 表直接 INSERT 覆盖，不需要先 DELETE。"""
        self._db.execute_write(
            """
            INSERT INTO vanna_store.vanna_prompt_config (config_key, config_value)
            VALUES (%s, %s)
            """,
            (key, value),
        )

    def _get_config_map(self) -> Dict[str, str]:
        rows = self._db.execute(
            "SELECT config_key, config_value FROM vanna_store.vanna_prompt_config"
        )
        return {row["config_key"]: row.get("config_value") or "" for row in rows}

    def _fallback_state(self, default_prompt: str = "") -> dict:
        """DB 不可用时返回最小可用状态，来源标记为 'fallback'。"""
        return {
            "prompt_versions": [
                {
                    "id": "default",
                    "name": "Default",
                    "description": "（Prompt DB 未初始化，显示配置文件版本）",
                    "system_prompt": default_prompt or "你是一个严谨的 Doris SQL 生成器。",
                    "created_at": "",
                    "updated_at": "",
                }
            ],
            "active_prompt_version": "default",
            "ab_test": {"enabled": False, "version_a": "default", "version_b": ""},
            "_source": "fallback",
        }

    # ─────────────────────────────────────────────────────────────────────────
    # 初始化种子数据
    # ─────────────────────────────────────────────────────────────────────────
    def ensure_seed(self, default_prompt: str = ""):
        """首次使用时写入默认 Prompt 版本和配置，已有数据则跳过。"""
        rows = self._db.execute("SELECT id FROM vanna_store.vanna_prompt LIMIT 1")
        if not rows:
            self.save_prompt_version({
                "id": "default",
                "name": "Default",
                "description": "当前主提示词版本",
                "system_prompt": default_prompt or "你是一个严谨的 Doris SQL 生成器。",
            })
            self.activate_prompt("default")

        config_map = self._get_config_map()
        if "ab_enabled" not in config_map:
            self.save_ab_test(False, "default", "")

    # ─────────────────────────────────────────────────────────────────────────
    # 读操作（容错：失败返回 fallback）
    # ─────────────────────────────────────────────────────────────────────────
    def list_prompt_versions(self) -> List[dict]:
        rows = self._db.execute(
            """
            SELECT id, name, description, system_prompt, created_at, updated_at
            FROM vanna_store.vanna_prompt
            ORDER BY updated_at DESC, id ASC
            """
        )
        return [
            {
                "id": row["id"],
                "name": row.get("name") or row["id"],
                "description": row.get("description") or "",
                "system_prompt": row.get("system_prompt") or "",
                "created_at": str(row.get("created_at") or ""),
                "updated_at": str(row.get("updated_at") or ""),
            }
            for row in rows
        ]

    def get_prompt_state(self, default_prompt: str = "") -> dict:
        """
        读取完整 Prompt Lab 状态。
        DB 不可用（表不存在、连接失败）时返回 fallback 状态，不抛异常。
        """
        try:
            self.ensure_seed(default_prompt)
            config_map = self._get_config_map()
            active = config_map.get("active_prompt_version") or "default"
            return {
                "prompt_versions": self.list_prompt_versions(),
                "active_prompt_version": active,
                "ab_test": {
                    "enabled": config_map.get("ab_enabled", "false") == "true",
                    "version_a": config_map.get("ab_version_a") or active,
                    "version_b": config_map.get("ab_version_b") or "",
                },
                "_source": "db",
            }
        except Exception as exc:
            logger.warning(
                f"[PromptStore] DB 不可用，返回 fallback 状态: {exc}"
            )
            return self._fallback_state(default_prompt)

    # ─────────────────────────────────────────────────────────────────────────
    # 写操作（失败抛出原始异常，由调用方决定响应）
    # ─────────────────────────────────────────────────────────────────────────
    def save_prompt_version(self, payload: dict):
        version_id = payload["id"].strip()
        # Doris UNIQUE KEY 表：先删后插保证幂等（INSERT 覆盖在部分版本有延迟）
        self._db.execute_write(
            "DELETE FROM vanna_store.vanna_prompt WHERE id = %s",
            (version_id,),
        )
        self._db.execute_write(
            """
            INSERT INTO vanna_store.vanna_prompt (id, name, description, system_prompt)
            VALUES (%s, %s, %s, %s)
            """,
            (
                version_id,
                payload.get("name") or version_id,
                payload.get("description") or "",
                payload.get("system_prompt") or "",
            ),
        )

    def activate_prompt(self, version_id: str):
        self._set_config("active_prompt_version", version_id)

    def save_ab_test(self, enabled: bool, version_a: str, version_b: str):
        self._set_config("ab_enabled", "true" if enabled else "false")
        self._set_config("ab_version_a", version_a)
        self._set_config("ab_version_b", version_b)
