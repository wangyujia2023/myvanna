"""
Qwen 客户端 — 兼容 OpenAI 格式，使用 DashScope API
LLM:       qwen-plus / qwen-turbo / qwen-max（降级链）
Embedding: text-embedding-v3（1024 维）

解决限速：Embedding 内存缓存 + 指数退避重试 + 模型降级链
"""
import hashlib
import logging
import threading
import time
from typing import List, Optional

from openai import OpenAI
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_random_exponential,
    before_sleep_log,
    RetryError,
)

logger = logging.getLogger(__name__)

_BASE_URL = "https://dashscope.aliyuncs.com/compatible-mode/v1"

# ── 模型降级链 ──────────────────────────────────────────────────────────────
_LLM_CHAIN = [
    "qwen-plus",
    "qwen-turbo",
    "qwen-max",
]


# ── 哪些异常触发重试 ─────────────────────────────────────────────────────────
def _is_retryable(exc: Exception) -> bool:
    msg = str(exc).lower()
    return any(k in msg for k in [
        "429", "503", "quota", "rate limit", "overloaded",
        "resource exhausted", "service unavailable", "too many requests",
        "throttl",
    ])


class EmbeddingCache:
    """线程安全 LRU 内存缓存（相同文本不重复调用 API）"""

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
                remove_keys = list(self._cache.keys())[: self._maxsize // 5]
                for rk in remove_keys:
                    del self._cache[rk]
            self._cache[k] = vec

    @property
    def size(self) -> int:
        return len(self._cache)


class QwenClient:
    """
    通义千问 API 封装，接口与原 GeminiClient 保持一致：
    - get_embedding(text) → List[float]
    - generate(prompt)    → str
    """

    def __init__(self, api_key: str,
                 model: str = "qwen-plus",
                 embedding_model: str = "text-embedding-v3",
                 max_concurrent: int = 5):
        self._client = OpenAI(api_key=api_key, base_url=_BASE_URL)
        self.model_name = model
        self.embedding_model = embedding_model
        self._cache = EmbeddingCache()
        self._semaphore = threading.Semaphore(max_concurrent)
        self._stats = {"embed_calls": 0, "embed_cache_hits": 0,
                       "llm_calls": 0, "retries": 0}

    # ─────────────────────────────────────────────────────────────────────────
    # Embedding
    # ─────────────────────────────────────────────────────────────────────────
    def get_embedding(self, text: str) -> List[float]:
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
        wait=wait_random_exponential(multiplier=1, min=2, max=60),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _embed_with_retry(self, text: str) -> List[float]:
        with self._semaphore:
            t0 = time.time()
            resp = self._client.embeddings.create(
                model=self.embedding_model,
                input=text,
                encoding_format="float",
            )
            vec = resp.data[0].embedding
            elapsed = (time.time() - t0) * 1000
            logger.debug(f"[Embedding] {elapsed:.0f}ms, dims={len(vec)}")
            return vec

    # ─────────────────────────────────────────────────────────────────────────
    # LLM 生成（带降级链）
    # ─────────────────────────────────────────────────────────────────────────
    def generate(self, prompt: str, temperature: float = 0.05) -> str:
        self._stats["llm_calls"] += 1
        models_to_try = [self.model_name] + [
            m for m in _LLM_CHAIN if m != self.model_name
        ]

        last_exc = None
        for model_name in models_to_try:
            try:
                return self._generate_with_retry(prompt, model_name, temperature)
            except RetryError as e:
                last_exc = e
                self._stats["retries"] += 1
                logger.warning(f"[LLM] {model_name} 重试耗尽，切换下一个模型")
            except Exception as e:
                if _is_retryable(e):
                    last_exc = e
                    self._stats["retries"] += 1
                    logger.warning(f"[LLM] {model_name} 失败: {e}，尝试下一个模型")
                else:
                    raise

        raise RuntimeError(
            f"所有模型均失败: {models_to_try}. 最后错误: {last_exc}"
        )

    @retry(
        retry=retry_if_exception(_is_retryable),
        stop=stop_after_attempt(4),
        wait=wait_random_exponential(multiplier=1.5, min=3, max=60),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )
    def _generate_with_retry(self, prompt: str, model_name: str,
                              temperature: float) -> str:
        with self._semaphore:
            t0 = time.time()
            resp = self._client.chat.completions.create(
                model=model_name,
                messages=[{"role": "user", "content": prompt}],
                temperature=temperature,
                max_tokens=4096,
            )
            elapsed = (time.time() - t0) * 1000
            logger.info(f"[LLM] {model_name} {elapsed:.0f}ms")
            return resp.choices[0].message.content

    # ─────────────────────────────────────────────────────────────────────────
    # 统计
    # ─────────────────────────────────────────────────────────────────────────
    @property
    def stats(self) -> dict:
        total = self._stats["embed_calls"] + self._stats["embed_cache_hits"]
        hit_rate = (self._stats["embed_cache_hits"] / total * 100) if total else 0.0
        return {
            **self._stats,
            "cache_size": self._cache.size,
            "embed_cache_hit_rate": f"{hit_rate:.1f}%",
        }
