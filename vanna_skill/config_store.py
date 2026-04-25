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
    "initial_prompt": "",
}


def load_config() -> dict:
    if not CONFIG_PATH.exists():
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG.copy()

    with CONFIG_PATH.open("r", encoding="utf-8") as f:
        data = json.load(f)

    config = DEFAULT_CONFIG.copy()
    config.update(data)
    return config


def save_config(config: dict) -> dict:
    merged = DEFAULT_CONFIG.copy()
    merged.update(config)

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
