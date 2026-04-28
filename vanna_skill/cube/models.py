from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class CubeModel:
    cube_name: str
    title: str = ""
    sql_table: str = ""
    sql_expression: str = ""
    data_source: str = "default"
    public_flag: bool = True
    visible: bool = True


@dataclass
class CubeMeasure:
    cube_name: str
    measure_name: str
    title: str
    sql_expr: str
    measure_type: str
    description: str = ""
    format: str = "number"
    drill_members: List[str] = field(default_factory=list)
    visible: bool = True


@dataclass
class CubeDimension:
    cube_name: str
    dimension_name: str
    title: str
    sql_expr: str
    dimension_type: str
    description: str = ""
    primary_key_flag: bool = False
    enum_mapping: Dict[str, Any] = field(default_factory=dict)
    hierarchy: List[str] = field(default_factory=list)
    visible: bool = True


@dataclass
class CubeDimensionValue:
    cube_name: str
    dimension_name: str
    value_code: str
    value_label: str
    aliases: List[str] = field(default_factory=list)
    source: str = "manual"
    usage_count: int = 0
    visible: bool = True


@dataclass
class CubeJoin:
    cube_name: str
    target_cube: str
    relationship: str
    join_type: str
    join_sql: str
    visible: bool = True


@dataclass
class CubeSegment:
    cube_name: str
    segment_name: str
    title: str
    filter_sql: str
    description: str = ""
    visible: bool = True


@dataclass
class CubeTemplate:
    template_name: str
    template_type: str
    title: str
    template_sql: str
    params: Dict[str, Any] = field(default_factory=dict)
    visible: bool = True


@dataclass
class CubeSemanticAlias:
    entity_type: str
    entity_name: str
    alias_text: str
    source: str = "manual"
    weight: float = 1.0
    match_type: str = "contains"
    visible: bool = True


@dataclass
class CubeBundle:
    models: List[CubeModel] = field(default_factory=list)
    measures: List[CubeMeasure] = field(default_factory=list)
    dimensions: List[CubeDimension] = field(default_factory=list)
    dimension_values: List[CubeDimensionValue] = field(default_factory=list)
    joins: List[CubeJoin] = field(default_factory=list)
    segments: List[CubeSegment] = field(default_factory=list)
    templates: List[CubeTemplate] = field(default_factory=list)
    aliases: List[CubeSemanticAlias] = field(default_factory=list)
    version_no: int = 0
    checksum: str = ""
