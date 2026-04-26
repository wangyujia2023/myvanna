"""
召回融合 agent：并行 skill 结果归一。
"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List

from .base import BaseAgent
from ..skills.base import BaseSkill, SkillContext


class RecallFusionAgent(BaseAgent):
    name = "recall_fusion_agent"

    def __init__(self, skills: List[BaseSkill]):
        self._skills = skills

    def run(self, context: SkillContext) -> Dict[str, object]:
        outputs: Dict[str, object] = {}
        with ThreadPoolExecutor(max_workers=max(1, len(self._skills))) as executor:
            futures = {
                executor.submit(skill.run, context): skill.name
                for skill in self._skills
            }
            for future in as_completed(futures):
                name = futures[future]
                outputs[name] = future.result()

        sql_examples = outputs.get("sql_example_skill", {}).get("items", [])
        ddl_items = outputs.get("doris_schema_skill", {}).get("items", [])
        doc_items = outputs.get("business_doc_skill", {}).get("items", [])
        audit_items = outputs.get("audit_pattern_skill", {}).get("items", [])
        lineage_items = outputs.get("lineage_skill", {}).get("items", [])

        fused_context = {
            "sql_examples": sql_examples,
            "ddl_items": ddl_items,
            "doc_items": doc_items,
            "audit_items": audit_items,
            "lineage_items": lineage_items,
            "weights": {
                "ddl": 0.4,
                "sql_examples": 0.4,
                "business_doc": 0.2,
                "audit_patterns": 0.2,
                "lineage": 0.3,
            },
        }
        return {"skill_outputs": outputs, "fused_context": fused_context}
