from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List

from .models import CubeBundle


class CubeModelRenderer:
    def __init__(self, target_dir: Path):
        self._target_dir = target_dir

    def render_bundle(self, bundle: CubeBundle) -> Dict[str, object]:
        self._target_dir.mkdir(parents=True, exist_ok=True)

        measures_by_cube: Dict[str, List[dict]] = {}
        dims_by_cube: Dict[str, List[dict]] = {}
        joins_by_cube: Dict[str, List[dict]] = {}
        segments_by_cube: Dict[str, List[dict]] = {}

        for item in bundle.measures:
            measures_by_cube.setdefault(item.cube_name, []).append(item.__dict__)
        for item in bundle.dimensions:
            dims_by_cube.setdefault(item.cube_name, []).append(item.__dict__)
        for item in bundle.joins:
            joins_by_cube.setdefault(item.cube_name, []).append(item.__dict__)
        for item in bundle.segments:
            segments_by_cube.setdefault(item.cube_name, []).append(item.__dict__)

        files: List[str] = []
        for model in bundle.models:
            path = self._target_dir / f"{model.cube_name}.js"
            path.write_text(
                self._render_cube_file(
                    model.__dict__,
                    measures_by_cube.get(model.cube_name, []),
                    dims_by_cube.get(model.cube_name, []),
                    joins_by_cube.get(model.cube_name, []),
                    segments_by_cube.get(model.cube_name, []),
                ),
                encoding="utf-8",
            )
            files.append(path.name)

        manifest = {
            "version_no": bundle.version_no,
            "checksum": bundle.checksum,
            "files": files,
        }
        (self._target_dir / "_bundle.json").write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        return manifest

    def _render_cube_file(
        self,
        model: dict,
        measures: List[dict],
        dimensions: List[dict],
        joins: List[dict],
        segments: List[dict],
    ) -> str:
        lines = []
        lines.append(f"cube(`{model['cube_name']}`, {{")
        if model.get("sql_expression"):
            lines.append(f"  sql: `{model['sql_expression']}`,")
        else:
            lines.append(f"  sql_table: `{model['sql_table']}`,")
        lines.append(f"  title: `{model.get('title') or model['cube_name']}`,")
        lines.append(f"  public: {str(bool(model.get('public_flag', True))).lower()},")
        lines.append("")

        if joins:
            lines.append("  joins: {")
            for join in joins:
                lines.append(f"    {join['target_cube']}: {{")
                lines.append(f"      sql: `{join['join_sql']}`,")
                lines.append(f"      relationship: `{join['relationship']}`")
                lines.append("    },")
            lines.append("  },")
            lines.append("")

        lines.append("  measures: {")
        for measure in measures:
            lines.append(f"    {measure['measure_name']}: {{")
            lines.append(f"      sql: `{measure['sql_expr']}`,")
            lines.append(f"      type: `{measure['measure_type']}`,")
            lines.append(f"      title: `{measure.get('title') or measure['measure_name']}`,")
            if measure.get("format"):
                lines.append(f"      format: `{measure['format']}`,")
            lines.append("    },")
        lines.append("  },")
        lines.append("")

        lines.append("  dimensions: {")
        for dim in dimensions:
            lines.append(f"    {dim['dimension_name']}: {{")
            lines.append(f"      sql: `{dim['sql_expr']}`,")
            lines.append(f"      type: `{dim['dimension_type']}`,")
            lines.append(f"      title: `{dim.get('title') or dim['dimension_name']}`,")
            if dim.get("primary_key_flag"):
                lines.append("      primary_key: true,")
            lines.append("    },")
        lines.append("  },")
        lines.append("")

        if segments:
            lines.append("  segments: {")
            for segment in segments:
                lines.append(f"    {segment['segment_name']}: {{")
                lines.append(f"      sql: `{segment['filter_sql']}`")
                lines.append("    },")
            lines.append("  },")
        lines.append("});")
        lines.append("")
        return "\n".join(lines)
