"""
Skill 抽象基类。
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass
class SkillContext:
    question: str
    normalized_query: str = ""
    intent: str = "data_query"
    entity: str = ""
    metadata: Dict[str, Any] = field(default_factory=dict)


class BaseSkill(ABC):
    name = "base_skill"

    @abstractmethod
    def run(self, context: SkillContext) -> Dict[str, Any]:
        raise NotImplementedError
