from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List

from ..doris_client import DorisClient

logger = logging.getLogger(__name__)


@dataclass
class RCANode:
    node_name: str
    node_type: str
    title: str = ""
    description: str = ""
    cube_ref: str = ""
    expression: str = ""
    enabled: bool = True


@dataclass
class RCAEdge:
    source_node: str
    target_node: str
    edge_type: str = "driver"
    direction: str = "unknown"
    prior_strength: float = 0.0
    confidence: float = 0.5
    lag: str = "P0D"
    condition: Dict[str, Any] = field(default_factory=dict)
    evidence: Dict[str, Any] = field(default_factory=dict)
    enabled: bool = True

    @property
    def prior_score(self) -> float:
        return abs(self.prior_strength) * self.confidence


@dataclass
class RCACausalSpec:
    treatment_node: str
    outcome_node: str
    common_causes: List[str] = field(default_factory=list)
    instruments: List[str] = field(default_factory=list)
    effect_modifiers: List[str] = field(default_factory=list)
    graph_gml: str = ""
    estimator: str = "backdoor.linear_regression"
    refuters: List[str] = field(default_factory=list)
    enabled: bool = True


@dataclass
class RCAGraphBundle:
    nodes: List[RCANode] = field(default_factory=list)
    edges: List[RCAEdge] = field(default_factory=list)
    causal_specs: List[RCACausalSpec] = field(default_factory=list)

    def driver_edges_for(self, target_node: str) -> List[RCAEdge]:
        target = (target_node or "").strip()
        edges = [edge for edge in self.edges if edge.target_node == target and edge.enabled]
        edges.sort(key=lambda edge: edge.prior_score, reverse=True)
        return edges

    def node_map(self) -> Dict[str, RCANode]:
        return {node.node_name: node for node in self.nodes if node.enabled}


class RCAGraphStore:
    """Doris-backed RCA graph repository.

    The graph is stored as ordinary Doris tables, then represented in Python as
    nodes and directed edges. This keeps the architecture lightweight while
    staying compatible with DoWhy's graph/common_causes/effect_modifiers model.
    """

    def __init__(self, client: DorisClient, database: str = "rca_store"):
        self._db = client
        self._database = database or "rca_store"

    def load_bundle(self) -> RCAGraphBundle:
        return RCAGraphBundle(
            nodes=self._load_nodes(),
            edges=self._load_edges(),
            causal_specs=self._load_causal_specs(),
        )

    def summary(self) -> Dict[str, Any]:
        bundle = self.load_bundle()
        node_types: Dict[str, int] = {}
        edge_types: Dict[str, int] = {}
        for node in bundle.nodes:
            node_types[node.node_type] = node_types.get(node.node_type, 0) + 1
        for edge in bundle.edges:
            edge_types[edge.edge_type] = edge_types.get(edge.edge_type, 0) + 1
        return {
            "database": self._database,
            "node_count": len(bundle.nodes),
            "edge_count": len(bundle.edges),
            "causal_spec_count": len(bundle.causal_specs),
            "node_types": node_types,
            "edge_types": edge_types,
        }

    def graph_for_metric(self, metric_name: str) -> Dict[str, Any]:
        bundle = self.load_bundle()
        node_map = bundle.node_map()
        edges = bundle.driver_edges_for(metric_name)
        return {
            "metric": metric_name,
            "target": self._node_payload(node_map.get(metric_name)),
            "drivers": [
                {
                    **self._edge_payload(edge),
                    "source": self._node_payload(node_map.get(edge.source_node)),
                }
                for edge in edges
            ],
            "causal_specs": [
                self._causal_spec_payload(spec)
                for spec in bundle.causal_specs
                if spec.outcome_node == metric_name and spec.enabled
            ],
        }

    def to_gml(self, metric_name: str = "") -> str:
        bundle = self.load_bundle()
        selected_nodes = {node.node_name for node in bundle.nodes if node.enabled}
        selected_edges = [edge for edge in bundle.edges if edge.enabled]
        if metric_name:
            selected_edges = [
                edge for edge in selected_edges
                if edge.target_node == metric_name or edge.source_node == metric_name
            ]
            selected_nodes = {metric_name}
            for edge in selected_edges:
                selected_nodes.add(edge.source_node)
                selected_nodes.add(edge.target_node)
        lines = ["graph [", "  directed 1"]
        for name in sorted(selected_nodes):
            lines.extend(["  node [", f'    id "{name}"', f'    label "{name}"', "  ]"])
        for edge in selected_edges:
            lines.extend([
                "  edge [",
                f'    source "{edge.source_node}"',
                f'    target "{edge.target_node}"',
                f'    label "{edge.edge_type}"',
                "  ]",
            ])
        lines.append("]")
        return "\n".join(lines)

    def _load_nodes(self) -> List[RCANode]:
        rows = self._execute_optional(
            f"""
            SELECT node_name, node_type, title, description, cube_ref, expression, enabled
            FROM {self._database}.rca_nodes
            WHERE enabled = 1
            ORDER BY node_type, node_name
            """
        )
        return [
            RCANode(
                node_name=str(row.get("node_name") or ""),
                node_type=str(row.get("node_type") or ""),
                title=str(row.get("title") or ""),
                description=str(row.get("description") or ""),
                cube_ref=str(row.get("cube_ref") or ""),
                expression=str(row.get("expression") or ""),
                enabled=bool(row.get("enabled", 1)),
            )
            for row in rows
        ]

    def _load_edges(self) -> List[RCAEdge]:
        rows = self._execute_optional(
            f"""
            SELECT source_node, target_node, edge_type, direction, prior_strength,
                   confidence, lag, condition_json, evidence_json, enabled
            FROM {self._database}.rca_edges
            WHERE enabled = 1
            ORDER BY target_node, ABS(prior_strength * confidence) DESC, source_node
            """
        )
        return [
            RCAEdge(
                source_node=str(row.get("source_node") or ""),
                target_node=str(row.get("target_node") or ""),
                edge_type=str(row.get("edge_type") or "driver"),
                direction=str(row.get("direction") or "unknown"),
                prior_strength=float(row.get("prior_strength", 0.0) or 0.0),
                confidence=float(row.get("confidence", 0.5) or 0.5),
                lag=str(row.get("lag") or "P0D"),
                condition=_json_dict(row.get("condition_json")),
                evidence=_json_dict(row.get("evidence_json")),
                enabled=bool(row.get("enabled", 1)),
            )
            for row in rows
        ]

    def _load_causal_specs(self) -> List[RCACausalSpec]:
        rows = self._execute_optional(
            f"""
            SELECT treatment_node, outcome_node, common_causes_json, instruments_json,
                   effect_modifiers_json, graph_gml, estimator, refuters_json, enabled
            FROM {self._database}.rca_causal_specs
            WHERE enabled = 1
            ORDER BY outcome_node, treatment_node
            """
        )
        return [
            RCACausalSpec(
                treatment_node=str(row.get("treatment_node") or ""),
                outcome_node=str(row.get("outcome_node") or ""),
                common_causes=_json_list(row.get("common_causes_json")),
                instruments=_json_list(row.get("instruments_json")),
                effect_modifiers=_json_list(row.get("effect_modifiers_json")),
                graph_gml=str(row.get("graph_gml") or ""),
                estimator=str(row.get("estimator") or "backdoor.linear_regression"),
                refuters=_json_list(row.get("refuters_json")),
                enabled=bool(row.get("enabled", 1)),
            )
            for row in rows
        ]

    def _execute_optional(self, sql: str) -> List[dict]:
        try:
            return self._db.execute(sql)
        except Exception as exc:
            logger.warning(
                "[RCAGraphStore] 加载 RCA 图谱失败，返回空图。请执行 sql/rca_store.sql。error=%s",
                exc,
            )
            return []

    def _node_payload(self, node: RCANode | None) -> Dict[str, Any]:
        if not node:
            return {}
        return {
            "node_name": node.node_name,
            "node_type": node.node_type,
            "title": node.title,
            "description": node.description,
            "cube_ref": node.cube_ref,
            "expression": node.expression,
        }

    def _edge_payload(self, edge: RCAEdge) -> Dict[str, Any]:
        return {
            "source_node": edge.source_node,
            "target_node": edge.target_node,
            "edge_type": edge.edge_type,
            "direction": edge.direction,
            "prior_strength": edge.prior_strength,
            "confidence": edge.confidence,
            "prior_score": edge.prior_score,
            "lag": edge.lag,
            "condition": edge.condition,
            "evidence": edge.evidence,
        }

    def _causal_spec_payload(self, spec: RCACausalSpec) -> Dict[str, Any]:
        return {
            "treatment_node": spec.treatment_node,
            "outcome_node": spec.outcome_node,
            "common_causes": spec.common_causes,
            "instruments": spec.instruments,
            "effect_modifiers": spec.effect_modifiers,
            "graph_gml": spec.graph_gml,
            "estimator": spec.estimator,
            "refuters": spec.refuters,
        }


def _json_list(value: Any) -> List[Any]:
    if not value:
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


def _json_dict(value: Any) -> Dict[str, Any]:
    if not value:
        return {}
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}
