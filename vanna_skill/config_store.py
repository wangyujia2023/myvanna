"""
统一配置读写：
- 仓库根目录 config.json 作为唯一配置源
- API、训练脚本、管理台都读取同一份配置
"""
import json
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT_DIR / "config.json"

DEFAULT_CONFIG = {
    "host": "10.26.20.3",
    "port": 19030,
    "user": "root",
    "password": "",
    "database": "retail_dw",
    "qwen_api_key": "",
    "langsmith_api_key": "",
    "model": "qwen-plus",
    "embedding_model": "text-embedding-v3",
    "n_results": 5,
    "embedding_fallback_mode": "keyword",  # "keyword" | "fail"
    "initial_prompt": "",
    "prompt_versions": [],
    "active_prompt_version": "default",
    "ab_test": {
        "enabled": False,
        "version_a": "default",
        "version_b": "",
    },
}


def _normalize_prompt_config(config: dict) -> dict:
    prompt_versions = config.get("prompt_versions") or []
    initial_prompt = config.get("initial_prompt", "")

    normalized_versions = []
    seen_ids = set()
    for item in prompt_versions:
        version_id = (item or {}).get("id", "").strip()
        if not version_id or version_id in seen_ids:
            continue
        seen_ids.add(version_id)
        normalized_versions.append({
            "id": version_id,
            "name": (item or {}).get("name", version_id),
            "description": (item or {}).get("description", ""),
            "system_prompt": (item or {}).get("system_prompt", ""),
        })

    if "default" not in seen_ids:
        normalized_versions.insert(0, {
            "id": "default",
            "name": "Default",
            "description": "当前主提示词版本",
            "system_prompt": initial_prompt,
        })
    elif initial_prompt:
        normalized_versions = [
            {
                **item,
                "system_prompt": initial_prompt if item["id"] == "default" else item["system_prompt"],
            }
            for item in normalized_versions
        ]

    active_prompt_version = config.get("active_prompt_version") or "default"
    valid_ids = {item["id"] for item in normalized_versions}
    if active_prompt_version not in valid_ids:
        active_prompt_version = "default"

    ab_test = config.get("ab_test") or {}
    version_a = ab_test.get("version_a") or active_prompt_version
    if version_a not in valid_ids:
        version_a = active_prompt_version
    version_b = ab_test.get("version_b") or ""
    if version_b and version_b not in valid_ids:
        version_b = ""

    config["prompt_versions"] = normalized_versions
    config["active_prompt_version"] = active_prompt_version
    config["initial_prompt"] = next(
        (item["system_prompt"] for item in normalized_versions if item["id"] == "default"),
        initial_prompt,
    )
    config["ab_test"] = {
        "enabled": bool(ab_test.get("enabled")),
        "version_a": version_a,
        "version_b": version_b,
    }
    return config


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        save_config(DEFAULT_CONFIG)
        return _normalize_prompt_config(DEFAULT_CONFIG.copy())

    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    config = DEFAULT_CONFIG.copy()
    config.update(data)
    return _normalize_prompt_config(config)


def save_config(config: dict) -> dict:
    merged = DEFAULT_CONFIG.copy()
    merged.update(config)
    merged = _normalize_prompt_config(merged)

    with CONFIG_PATH.open("w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
        f.write("\n")

    return merged


def mask_secret(secret: str) -> str:
    if not secret:
        return ""
    if len(secret) <= 8:
        return "*" * len(secret)
    return secret[:8] + "..."
