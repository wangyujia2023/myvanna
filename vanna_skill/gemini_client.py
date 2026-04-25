"""
Gemini 客户端 — LLM 用 google.genai SDK，Embedding 用直接 REST 调用
解决 429/503：Embedding 缓存 + 指数退避重试 + 模型降级链 + 并发限流

注意：google.genai SDK 的 embed_content 内部使用 batchEmbedContents，
      但 text-embedding-004 只支持 embedContent（单条）接口，因此
      Embedding 部分绕过 SDK，直接调 REST API。
"""
import hashlib
import logging
import threading
import time
from typing import List, Optional

import requests as _requests

from google import genai
from google.genai import types
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_random_exponential,
    before_sleep_log,
    RetryError,
)

logger = logging.getLogger(__name__)

# Embedding REST 端点（v1beta 支持 embedContent 单条接口）
_EMBED_URL = (
    "https://generativelanguage.googleapis.com"
    "/v1beta/models/{model}:embedContent?key={key}"
)

# ── 哪些异常触发重试 ─────────────────────────────────────────────────────────
def _is_retryable(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(k in msg for k in [
        "429", "503", "quota", "rate limit", "overloaded",
        "resource exhausted", "service unavailable", "too many requests",
    ])


# ── 模型降级链 ──────────────────────────────────────────────────────────────
_FLASH_CHAIN = [
    "gemini-2.0-flash",
    "gemini-1.5-flash",
    "gemini-1.5-flash-8b",
]


class EmbeddingCache:
    """线程安全的 LRU 式 Embedding 内存缓存（相同文本不重复调用 API）"""

    def __init__(self, maxsize: int = 2000):
        self._cache: dict[str, List[float]] = {}
        self._lock = threading.Lock()
        self._maxsize = maxsize

    def _key(self, text: str) -> str:
        return hashlib.md5(text.encode("utf-8")).hexdigest()

    def get(self, text: str) -> Optional[List[float]]:
        return self._cache.get(self._key(text))

    def set(self, text: str, vec: List[float]):
        k = self._key(text)
        with self._lock:
            if len(self._cache) >= self._maxsize:
                # 淘汰最旧的 20%
                remove_keys = list(self._cache.keys())[: self._maxsize // 5]
                for rk in remove_keys:
                    del self._cache[rk]
            self._cache[k] = vec

    @property
    def size(self) -> int:
        return len(self._cache)


class GeminiClient:
    """
    封装 Gemini API，提供：
    - get_embedding(text)  直接 REST + 缓存 + 自动重试
    - generate(prompt)     google.genai SDK + 模型降级链 + 自动重试
    """

    def __init__(self, api_key: str, model: str = "gemini-2.0-flash",
                 embedding_model: str = "text-embedding-004",
                 max_concurrent: int = 3):
        self._api_key = api_key
        # SDK 仅用于 LLM 生成（generate_content）
        self._client = genai.Client(api_key=api_key)
        self.model_name = model
        self.embedding_model = embedding_model
        self._cache = EmbeddingCache()
        # 并发限流：同时最多 max_concurrent 个在途 Gemini 请求
        self._semaphore = threading.Semaphore(max_concurrent)
        self._stats = {"embed_calls": 0, "embed_cache_hits": 0,
                       "llm_calls": 0, "retries": 0}

    # ─────────────────────────────────────────────────────────────────────────
    # Embedding（直接 REST，绕过 SDK 的 batchEmbedContents）
    # ─────────────────────────────────────────────────────────────────────────
    def get_embedding(self, text: str) -> List[float]:
        """获取文本向量，自动使用缓存"""
        cached = self._cache.get(text)
        if cached is not None:
            self._stats["embed_cache_hits"] += 1
            logger.debug(f"[Embedding] cache hit: {text[:40]!r}")
            return cached

        vec = self._embed_with_retry(text)
        self._cache.set(text, vec)
        self._stats["embed_calls"] += 1
        return vec

    @retry(
        retry=retry_if_exception(_is_retryable),
        stop=stop_after_attempt(6),
        wait=wait_random_exponential(multiplier=1.5, min=2, max=90),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _embed_with_retry(self, text: str) -> List[float]:
        """直接调 REST embedContent 接口（单条，非批量）"""
        with self._semaphore:
            t0 = time.time()
            url = _EMBED_URL.format(model=self.embedding_model, key=self._api_key)
            body = {
                "model": f"models/{self.embedding_model}",
                "content": {"parts": [{"text": text}]},
            }
            resp = _requests.post(url, json=body, timeout=30)
            if not resp.ok:
                # 让 tenacity 判断是否重试
                raise RuntimeError(
                    f"{resp.status_code} {resp.reason}. {resp.json()}"
                )
            values = resp.json()["embedding"]["values"]
            elapsed = (time.time() - t0) * 1000
            logger.debug(f"[Embedding] {elapsed:.0f}ms, dims={len(values)}")
            return values

    # ─────────────────────────────────────────────────────────────────────────
    # LLM 生成（带降级链，仍使用 SDK）
    # ─────────────────────────────────────────────────────────────────────────
    def generate(self, prompt: str, temperature: float = 0.05) -> str:
        """调用 LLM 生成文本，失败时按降级链自动切换模型"""
        self._stats["llm_calls"] += 1
        models_to_try = [self.model_name] + [
            m for m in _FLASH_CHAIN if m != self.model_name
        ]

        last_exc = None
        for model_name in models_to_try:
            try:
                return self._generate_with_retry(prompt, model_name, temperature)
            except RetryError as e:
                last_exc = e
                self._stats["retries"] += 1
                logger.warning(f"[LLM] {model_name} 重试耗尽，切换到下一个模型")
            except Exception as e:
                if _is_retryable(e):
                    last_exc = e
                    self._stats["retries"] += 1
                    logger.warning(f"[LLM] {model_name} 失败: {e}, 尝试下一个模型")
                else:
                    raise

        raise RuntimeError(
            f"所有模型均失败: {models_to_try}. 最后错误: {last_exc}"
        )

    @retry(
        retry=retry_if_exception(_is_retryable),
        stop=stop_after_attempt(5),
        wait=wait_random_exponential(multiplier=2, min=3, max=120),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _generate_with_retry(self, prompt: str, model_name: str,
                              temperature: float) -> str:
        with self._semaphore:
            t0 = time.time()
            response = self._client.models.generate_content(
                model=model_name,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=temperature,
                    max_output_tokens=4096,
                ),
            )
            elapsed = (time.time() - t0) * 1000
            logger.info(f"[LLM] {model_name} {elapsed:.0f}ms")
            return response.text

    # ─────────────────────────────────────────────────────────────────────────
    # 统计
    # ─────────────────────────────────────────────────────────────────────────
    @property
    def stats(self) -> dict:
        hit_rate = 0.0
        total = self._stats["embed_calls"] + self._stats["embed_cache_hits"]
        if total > 0:
            hit_rate = self._stats["embed_cache_hits"] / total * 100
        return {
            **self._stats,
            "cache_size": self._cache.size,
            "embed_cache_hit_rate": f"{hit_rate:.1f}%",
        }
