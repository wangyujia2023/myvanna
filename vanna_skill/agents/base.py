"""
Agent 抽象基类。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict


class BaseAgent(ABC):
    name = "base_agent"

    @abstractmethod
    def run(self, *args, **kwargs) -> Dict[str, Any]:
        raise NotImplementedError
