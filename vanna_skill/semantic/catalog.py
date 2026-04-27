"""
SemanticCatalog：语义知识目录（进程级单例）

DB 存储结构（独立数据库 semantic_store，4 张类型表）：
  semantic_store.vanna_semantic_entities    — EntityDef
  semantic_store.vanna_semantic_dimensions  — DimensionDef
  semantic_store.vanna_semantic_metrics     — MetricDef
  semantic_store.vanna_semantic_businesses  — BusinessDef

启动流程：
  1. 优先从 semantic_store 4 张表加载
  2. DB 为空时从 YAML 文件初始化并写入 DB
  3. /semantic/reload 接口可手动刷新

对外接口：
  get_catalog(sem_client)              → SemanticCatalog 单例
  invalidate_semantic_cache()          → 清除单例（下次请求重建）
"""
from __future__ import annotations

import json
import logging
import threading
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

import yaml

from .models import (
    BusinessDef, DimensionDef, EntityDef, JoinDef, MetricDef,
)

if TYPE_CHECKING:
    from ..doris_client import DorisClient

logger = logging.getLogger(__name__)

_YAML_DIR = Path(__file__).parent / "catalog_yaml"

# 进程级单例
_catalog_instance: Optional["SemanticCatalog"] = None
_catalog_lock = threading.Lock()


def _compact_sql(sql: str) -> str:
    return " ".join((sql or "").split())


# ─────────────────────────────────────────────────────────────────────────────
# 公共接口
# ─────────────────────────────────────────────────────────────────────────────

def get_catalog(
    sem_client: "DorisClient",
    db_name: str = "retail_dw",
) -> "SemanticCatalog":
    """返回进程级单例，首次调用时从 semantic_store DB 或 YAML 初始化。"""
    global _catalog_instance
    if _catalog_instance is not None:
        return _catalog_instance
    with _catalog_lock:
        if _catalog_instance is None:
            c = SemanticCatalog(sem_client, db_name=db_name)
            c.load()
            _catalog_instance = c
    return _catalog_instance


def invalidate_semantic_cache():
    """手动使缓存失效，下次请求重建。"""
    global _catalog_instance
    with _catalog_lock:
        _catalog_instance = None
    logger.info("[SemanticCatalog] 缓存已清除，下次请求将重建")


# ─────────────────────────────────────────────────────────────────────────────
# YAML 解析辅助
# ─────────────────────────────────────────────────────────────────────────────

def _parse_join(d: dict) -> JoinDef:
    return JoinDef(
        table=d.get("table", ""),
        alias=d.get("alias", ""),
        join_type=d.get("join_type", "LEFT JOIN"),
        on=d.get("on", ""),
    )


def _load_yaml_catalog(db_name: str) -> dict:
    yaml_path = _YAML_DIR / f"{db_name}.yaml"
    if not yaml_path.exists():
        logger.warning(f"[SemanticCatalog] YAML 文件不存在: {yaml_path}")
        return {}
    with open(yaml_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _parse_catalog(raw: dict) -> Tuple[
    Dict[str, EntityDef],
    Dict[str, DimensionDef],
    Dict[str, MetricDef],
    Dict[str, BusinessDef],
]:
    entities: Dict[str, EntityDef] = {}
    dimensions: Dict[str, DimensionDef] = {}
    metrics: Dict[str, MetricDef] = {}
    businesses: Dict[str, BusinessDef] = {}

    for item in raw.get("entities", []):
        e = EntityDef(
            name=item["name"],
            label=item["label"],
            description=item.get("description", ""),
            primary_table=item.get("primary_table", ""),
            primary_key=item.get("primary_key", ""),
            display_key=item.get("display_key", ""),
            searchable_fields=item.get("searchable_fields", []),
            synonyms=item.get("synonyms", []),
            tags=item.get("tags", []),
        )
        entities[e.name] = e

    for item in raw.get("dimensions", []):
        join_raw = item.get("join")
        d = DimensionDef(
            name=item["name"],
            label=item["label"],
            description=item.get("description", ""),
            dim_type=item.get("dim_type", "attribute"),
            entity=item.get("entity"),
            grain=item.get("grain"),
            expression=item.get("expression", ""),
            alias=item.get("alias", item["name"]),
            join=_parse_join(join_raw) if join_raw else None,
            select_fields=item.get("select_fields", []),
            synonyms=item.get("synonyms", []),
            tags=item.get("tags", []),
        )
        dimensions[d.name] = d

    for item in raw.get("metrics", []):
        extra_joins = [_parse_join(j) for j in item.get("extra_joins", [])]
        ps = item.get("primary_source")
        m = MetricDef(
            name=item["name"],
            label=item["label"],
            description=item.get("description", ""),
            metric_type=item.get("metric_type", "simple"),
            complexity=item.get("complexity", "normal"),
            expression=item.get("expression", ""),
            primary_source=ps,
            extra_joins=extra_joins,
            time_column=item.get("time_column", ""),
            numerator_expr=item.get("numerator_expr", ""),
            denominator_expr=item.get("denominator_expr", ""),
            output_format=item.get("output_format", "number"),
            unit=item.get("unit", ""),
            compatible_dimensions=item.get("compatible_dimensions", []),
            synonyms=item.get("synonyms", []),
            tags=item.get("tags", []),
        )
        metrics[m.name] = m

    for item in raw.get("business", []):
        b = BusinessDef(
            name=item["name"],
            label=item["label"],
            description=item.get("description", ""),
            related_metrics=item.get("related_metrics", []),
            related_dimensions=item.get("related_dimensions", []),
            typical_questions=item.get("typical_questions", []),
            default_dimensions=item.get("default_dimensions", []),
            default_sort=item.get("default_sort", ""),
            synonyms=item.get("synonyms", []),
            tags=item.get("tags", []),
        )
        businesses[b.name] = b

    return entities, dimensions, metrics, businesses


# ─────────────────────────────────────────────────────────────────────────────
# SemanticCatalog 主类
# ─────────────────────────────────────────────────────────────────────────────

class SemanticCatalog:
    """
    语义知识目录（内存常驻）

    加载优先级：DB（4 张类型表）→ YAML（DB 为空时初始化并写入 DB）
    内部字典使用私有属性（_entities / _dimensions / _metrics / _businesses），
    对外通过 get_*/match_* 方法访问。
    """

    def __init__(self, sem_client: "DorisClient", db_name: str = "retail_dw"):
        self._sem = sem_client     # 连接 semantic_store
        self._db_name = db_name

        self._entities: Dict[str, EntityDef] = {}
        self._dimensions: Dict[str, DimensionDef] = {}
        self._metrics: Dict[str, MetricDef] = {}
        self._businesses: Dict[str, BusinessDef] = {}

        # 同义词反查索引：token → List[(node_type, name)]
        self._synonym_index: Dict[str, List[Tuple[str, str]]] = {}

    # ── 初始化 ────────────────────────────────────────────────────────────────

    def load(self) -> "SemanticCatalog":
        """从 DB 加载，DB 为空时从 YAML 初始化。"""
        loaded = self._load_from_db()
        if not loaded:
            logger.info("[SemanticCatalog] DB 无数据，从 YAML 初始化...")
            self._load_from_yaml()
            self._save_to_db()
        self._build_synonym_index()
        logger.info(
            "[SemanticCatalog] 加载完成 entity=%d dim=%d metric=%d business=%d",
            len(self._entities), len(self._dimensions),
            len(self._metrics), len(self._businesses),
        )
        return self

    def reload(self) -> "SemanticCatalog":
        """强制从 YAML 重新加载并覆盖 DB。"""
        self._load_from_yaml()
        self._save_to_db()
        self._build_synonym_index()
        logger.info("[SemanticCatalog] 已从 YAML 重新加载")
        return self

    def refresh_from_db(self, required: bool = False) -> "SemanticCatalog":
        """
        强制从 semantic_store 重新加载到内存。
        required=True 时，如果 DB 中没有语义定义则抛错，不再回退 YAML。
        """
        loaded = self._load_from_db()
        self._build_synonym_index()
        if required and not loaded:
            raise ValueError(
                f"semantic_store 中没有找到 db_name={self._db_name} 的语义定义"
            )
        logger.info(
            "[SemanticCatalog] 已从 DB 刷新 entity=%d dim=%d metric=%d business=%d",
            len(self._entities),
            len(self._dimensions),
            len(self._metrics),
            len(self._businesses),
        )
        return self

    # ── DB 读取（4 张表分别查询）──────────────────────────────────────────────

    def _load_from_db(self) -> bool:
        """从 4 张类型表读取，返回是否成功加载到数据。"""
        try:
            self._entities = self._load_entities_from_db()
            self._dimensions = self._load_dimensions_from_db()
            self._metrics = self._load_metrics_from_db()
            self._businesses = self._load_businesses_from_db()
        except Exception as e:
            logger.warning("[SemanticCatalog] DB 读取失败: %s", e)
            return False
        return bool(
            self._entities or self._dimensions or self._metrics or self._businesses
        )

    def _load_entities_from_db(self) -> Dict[str, EntityDef]:
        sql = """
            SELECT name, label, description,
                   primary_table, primary_key, display_key,
                   searchable_fields_json, synonyms, tags
            FROM semantic_store.vanna_semantic_entities
            WHERE is_active = 1
              AND (db_name = %s OR IFNULL(db_name,'') = '')
            ORDER BY name
            """
        logger.info("[SemanticCatalog] DB query[entities]: %s | args=%s", _compact_sql(sql), (self._db_name,))
        rows = self._sem.execute(sql, (self._db_name,))
        result: Dict[str, EntityDef] = {}
        for r in rows:
            try:
                e = EntityDef(
                    name=r["name"],
                    label=r["label"],
                    description=r.get("description") or "",
                    primary_table=r.get("primary_table") or "",
                    primary_key=r.get("primary_key") or "",
                    display_key=r.get("display_key") or "",
                    searchable_fields=json.loads(r.get("searchable_fields_json") or "[]"),
                    synonyms=_split_csv(r.get("synonyms")),
                    tags=_split_csv(r.get("tags")),
                )
                result[e.name] = e
            except Exception as ex:
                logger.warning("[SemanticCatalog] 解析 entity 失败 %s: %s", r.get("name"), ex)
        return result

    def _load_dimensions_from_db(self) -> Dict[str, DimensionDef]:
        sql = """
            SELECT name, label, description, dim_type, entity, grain,
                   expression, alias,
                   join_table, join_alias, join_type, join_on,
                   select_fields_json, synonyms, tags
            FROM semantic_store.vanna_semantic_dimensions
            WHERE is_active = 1
              AND (db_name = %s OR IFNULL(db_name,'') = '')
            ORDER BY name
            """
        logger.info("[SemanticCatalog] DB query[dimensions]: %s | args=%s", _compact_sql(sql), (self._db_name,))
        rows = self._sem.execute(sql, (self._db_name,))
        result: Dict[str, DimensionDef] = {}
        for r in rows:
            try:
                join = None
                if r.get("join_table"):
                    join = JoinDef(
                        table=r["join_table"],
                        alias=r.get("join_alias") or "",
                        join_type=r.get("join_type") or "LEFT JOIN",
                        on=r.get("join_on") or "",
                    )
                d = DimensionDef(
                    name=r["name"],
                    label=r["label"],
                    description=r.get("description") or "",
                    dim_type=r.get("dim_type") or "attribute",
                    entity=r.get("entity"),
                    grain=r.get("grain"),
                    expression=r.get("expression") or "",
                    alias=r.get("alias") or r["name"],
                    join=join,
                    select_fields=json.loads(r.get("select_fields_json") or "[]"),
                    synonyms=_split_csv(r.get("synonyms")),
                    tags=_split_csv(r.get("tags")),
                )
                result[d.name] = d
            except Exception as ex:
                logger.warning("[SemanticCatalog] 解析 dimension 失败 %s: %s", r.get("name"), ex)
        return result

    def _load_metrics_from_db(self) -> Dict[str, MetricDef]:
        sql = """
            SELECT name, label, description, metric_type, complexity,
                   expression, primary_table, primary_alias,
                   extra_joins_json, time_column,
                   numerator_expr, denominator_expr,
                   output_format, unit,
                   compatible_dimensions_json, synonyms, tags,
                   IFNULL(query_pattern,'')      AS query_pattern,
                   IFNULL(template_params_json,'{}') AS template_params_json
            FROM semantic_store.vanna_semantic_metrics
            WHERE is_active = 1
              AND (db_name = %s OR IFNULL(db_name,'') = '')
            ORDER BY name
            """
        logger.info("[SemanticCatalog] DB query[metrics]: %s | args=%s", _compact_sql(sql), (self._db_name,))
        rows = self._sem.execute(sql, (self._db_name,))
        result: Dict[str, MetricDef] = {}
        for r in rows:
            try:
                extra_joins_raw = json.loads(r.get("extra_joins_json") or "[]")
                extra_joins = [_parse_join(j) for j in extra_joins_raw]
                ps = None
                if r.get("primary_table"):
                    ps = {
                        "table": r["primary_table"],
                        "alias": r.get("primary_alias") or "t",
                    }
                m = MetricDef(
                    name=r["name"],
                    label=r["label"],
                    description=r.get("description") or "",
                    metric_type=r.get("metric_type") or "simple",
                    complexity=r.get("complexity") or "normal",
                    expression=r.get("expression") or "",
                    primary_source=ps,
                    extra_joins=extra_joins,
                    time_column=r.get("time_column") or "",
                    numerator_expr=r.get("numerator_expr") or "",
                    denominator_expr=r.get("denominator_expr") or "",
                    output_format=r.get("output_format") or "number",
                    unit=r.get("unit") or "",
                    compatible_dimensions=json.loads(
                        r.get("compatible_dimensions_json") or "[]"
                    ),
                    synonyms=_split_csv(r.get("synonyms")),
                    tags=_split_csv(r.get("tags")),
                    query_pattern=r.get("query_pattern") or "",
                    template_params=json.loads(
                        r.get("template_params_json") or "{}"
                    ),
                )
                result[m.name] = m
            except Exception as ex:
                logger.warning("[SemanticCatalog] 解析 metric 失败 %s: %s", r.get("name"), ex)
        return result

    def _load_businesses_from_db(self) -> Dict[str, BusinessDef]:
        sql = """
            SELECT name, label, description,
                   related_metrics_json, related_dimensions_json,
                   typical_questions_json, default_dimensions_json,
                   default_sort, synonyms, tags
            FROM semantic_store.vanna_semantic_businesses
            WHERE is_active = 1
              AND (db_name = %s OR IFNULL(db_name,'') = '')
            ORDER BY name
            """
        logger.info("[SemanticCatalog] DB query[businesses]: %s | args=%s", _compact_sql(sql), (self._db_name,))
        rows = self._sem.execute(sql, (self._db_name,))
        result: Dict[str, BusinessDef] = {}
        for r in rows:
            try:
                b = BusinessDef(
                    name=r["name"],
                    label=r["label"],
                    description=r.get("description") or "",
                    related_metrics=json.loads(r.get("related_metrics_json") or "[]"),
                    related_dimensions=json.loads(r.get("related_dimensions_json") or "[]"),
                    typical_questions=json.loads(r.get("typical_questions_json") or "[]"),
                    default_dimensions=json.loads(r.get("default_dimensions_json") or "[]"),
                    default_sort=r.get("default_sort") or "",
                    synonyms=_split_csv(r.get("synonyms")),
                    tags=_split_csv(r.get("tags")),
                )
                result[b.name] = b
            except Exception as ex:
                logger.warning("[SemanticCatalog] 解析 business 失败 %s: %s", r.get("name"), ex)
        return result

    # ── YAML 读取 ──────────────────────────────────────────────────────────────

    def _load_from_yaml(self):
        raw = _load_yaml_catalog(self._db_name)
        if not raw:
            logger.warning("[SemanticCatalog] YAML 为空，跳过加载")
            return
        self._entities, self._dimensions, self._metrics, self._businesses = (
            _parse_catalog(raw)
        )

    # ── DB 写入（4 张表分别写入）──────────────────────────────────────────────

    def _save_to_db(self):
        """将当前内存状态全量覆盖写入 4 张类型表。"""
        self._save_entities_to_db()
        self._save_dimensions_to_db()
        self._save_metrics_to_db()
        self._save_businesses_to_db()
        logger.info(
            "[SemanticCatalog] 已写入 DB: entity=%d dim=%d metric=%d business=%d",
            len(self._entities), len(self._dimensions),
            len(self._metrics), len(self._businesses),
        )

    def _save_entities_to_db(self):
        self._clear_table("vanna_semantic_entities")
        rows = []
        for e in self._entities.values():
            rows.append((
                e.node_id, e.name, e.label, e.description or "",
                e.primary_table, e.primary_key, e.display_key,
                json.dumps(e.searchable_fields, ensure_ascii=False),
                ",".join(e.synonyms), ",".join(e.tags), self._db_name,
            ))
        self._bulk_insert(
            "vanna_semantic_entities",
            "(entity_id, name, label, description, primary_table, primary_key, "
            "display_key, searchable_fields_json, synonyms, tags, db_name)",
            rows,
        )

    def _save_dimensions_to_db(self):
        self._clear_table("vanna_semantic_dimensions")
        rows = []
        for d in self._dimensions.values():
            j = d.join
            rows.append((
                d.node_id, d.name, d.label, d.description or "",
                d.dim_type, d.entity or "", d.grain or "",
                d.expression or "", d.alias or d.name,
                j.table if j else "",
                j.alias if j else "",
                j.join_type if j else "LEFT JOIN",
                j.on if j else "",
                json.dumps(d.select_fields, ensure_ascii=False),
                ",".join(d.synonyms), ",".join(d.tags), self._db_name,
            ))
        self._bulk_insert(
            "vanna_semantic_dimensions",
            "(dim_id, name, label, description, dim_type, entity, grain, "
            "expression, alias, join_table, join_alias, join_type, join_on, "
            "select_fields_json, synonyms, tags, db_name)",
            rows,
        )

    def _save_metrics_to_db(self):
        self._clear_table("vanna_semantic_metrics")
        rows = []
        for m in self._metrics.values():
            extra_joins_json = json.dumps(
                [{"table": j.table, "alias": j.alias,
                  "join_type": j.join_type, "on": j.on}
                 for j in m.extra_joins],
                ensure_ascii=False,
            )
            rows.append((
                m.node_id, m.name, m.label, m.description or "",
                m.metric_type, m.complexity,
                m.expression or "",
                m.primary_table, m.primary_alias,
                extra_joins_json,
                m.time_column or "",
                m.numerator_expr or "", m.denominator_expr or "",
                m.output_format, m.unit or "",
                json.dumps(m.compatible_dimensions, ensure_ascii=False),
                ",".join(m.synonyms), ",".join(m.tags), self._db_name,
            ))
        self._bulk_insert(
            "vanna_semantic_metrics",
            "(metric_id, name, label, description, metric_type, complexity, "
            "expression, primary_table, primary_alias, extra_joins_json, "
            "time_column, numerator_expr, denominator_expr, output_format, unit, "
            "compatible_dimensions_json, synonyms, tags, db_name)",
            rows,
        )

    def _save_businesses_to_db(self):
        self._clear_table("vanna_semantic_businesses")
        rows = []
        for b in self._businesses.values():
            rows.append((
                b.node_id, b.name, b.label, b.description or "",
                json.dumps(b.related_metrics, ensure_ascii=False),
                json.dumps(b.related_dimensions, ensure_ascii=False),
                json.dumps(b.typical_questions, ensure_ascii=False),
                json.dumps(b.default_dimensions, ensure_ascii=False),
                b.default_sort or "",
                ",".join(b.synonyms), ",".join(b.tags), self._db_name,
            ))
        self._bulk_insert(
            "vanna_semantic_businesses",
            "(biz_id, name, label, description, related_metrics_json, "
            "related_dimensions_json, typical_questions_json, default_dimensions_json, "
            "default_sort, synonyms, tags, db_name)",
            rows,
        )

    # ── DB 工具方法 ───────────────────────────────────────────────────────────

    def _clear_table(self, table: str):
        try:
            self._sem.execute_write(
                f"DELETE FROM semantic_store.{table} "
                f"WHERE db_name = %s OR IFNULL(db_name,'') = ''",
                (self._db_name,),
            )
        except Exception as e:
            logger.warning("[SemanticCatalog] 清理 %s 失败: %s", table, e)

    def _bulk_insert(self, table: str, columns: str, rows: list, chunk: int = 50):
        for i in range(0, len(rows), chunk):
            batch = rows[i: i + chunk]
            ph = ",".join([f"({','.join(['%s'] * len(batch[0]))})" for _ in batch])
            args = [v for row in batch for v in row]
            try:
                self._sem.execute_write(
                    f"INSERT INTO semantic_store.{table} {columns} VALUES {ph}",
                    args,
                )
            except Exception as e:
                logger.warning("[SemanticCatalog] 写入 %s 失败: %s", table, e)

    # ── 同义词索引 ────────────────────────────────────────────────────────────

    def _build_synonym_index(self):
        idx: Dict[str, List[Tuple[str, str]]] = {}

        def _add(tokens: List[str], node_type: str, name: str):
            for raw in tokens:
                for tok in raw.split(","):
                    tok = tok.strip().lower()
                    if tok:
                        idx.setdefault(tok, []).append((node_type, name))

        for e in self._entities.values():
            _add([e.label] + e.synonyms, "entity", e.name)
        for d in self._dimensions.values():
            _add([d.label] + d.synonyms, "dimension", d.name)
        for m in self._metrics.values():
            _add([m.label] + m.synonyms, "metric", m.name)
        for b in self._businesses.values():
            _add([b.label] + b.synonyms, "business", b.name)

        self._synonym_index = idx

    # ── 查询接口 ──────────────────────────────────────────────────────────────

    def match_metrics(self, keywords: List[str]) -> List[str]:
        """
        根据关键词模糊匹配指标，返回 name 列表（按匹配优先级排序）。
        先精确同义词命中，再做标签/label 子串扫描。
        """
        found: Dict[str, int] = {}   # name → score（分越高越靠前）
        for kw in keywords:
            kw_l = kw.strip().lower()
            if not kw_l:
                continue
            # 精确同义词命中（score +2）
            for node_type, name in self._synonym_index.get(kw_l, []):
                if node_type == "metric":
                    found[name] = found.get(name, 0) + 2
            # label/synonyms 子串扫描（score +1）
            for m in self._metrics.values():
                if kw_l in m.label.lower() or any(kw_l in s.lower() for s in m.synonyms):
                    found[m.name] = found.get(m.name, 0) + 1
        return sorted(found, key=lambda n: -found[n])

    def match_dimensions(self, keywords: List[str]) -> List[str]:
        """根据关键词模糊匹配维度，返回 name 列表。"""
        found: Dict[str, int] = {}
        for kw in keywords:
            kw_l = kw.strip().lower()
            if not kw_l:
                continue
            for node_type, name in self._synonym_index.get(kw_l, []):
                if node_type == "dimension":
                    found[name] = found.get(name, 0) + 2
            for d in self._dimensions.values():
                if kw_l in d.label.lower() or any(kw_l in s.lower() for s in d.synonyms):
                    found[d.name] = found.get(d.name, 0) + 1
        return sorted(found, key=lambda n: -found[n])

    def get_metric(self, name: str) -> Optional[MetricDef]:
        return self._metrics.get(name)

    def get_dimension(self, name: str) -> Optional[DimensionDef]:
        return self._dimensions.get(name)

    def get_business(self, name: str) -> Optional[BusinessDef]:
        return self._businesses.get(name)

    def get_dimensions_for_metric(self, metric_name: str) -> List[DimensionDef]:
        m = self._metrics.get(metric_name)
        if not m:
            return []
        return [self._dimensions[d] for d in m.compatible_dimensions if d in self._dimensions]

    def coverage_score(self, metric_names: List[str], dimension_names: List[str]) -> float:
        """覆盖率 0~1：能在 Catalog 中命中的指标/维度数 ÷ 总识别数。"""
        total = len(metric_names) + len(dimension_names)
        if total == 0:
            return 0.0
        hit = (
            sum(1 for m in metric_names if m in self._metrics) +
            sum(1 for d in dimension_names if d in self._dimensions)
        )
        return hit / total

    # ── Prompt 摘要接口 ───────────────────────────────────────────────────────

    def metrics_summary(self, names: Optional[List[str]] = None) -> str:
        """
        供 Prompt 使用的指标摘要，返回格式化字符串。
        names: 若提供则只包含这些 name；否则输出全部 normal 指标。
        """
        if names is not None:
            pool = [self._metrics[n] for n in names if n in self._metrics]
        else:
            pool = [m for m in self._metrics.values() if m.complexity == "normal"]

        lines = []
        for m in pool:
            syns = "、".join(m.synonyms) if m.synonyms else ""
            syns_str = f"（同义：{syns}）" if syns else ""
            lines.append(
                f"  - {m.name}：{m.label}{syns_str}"
                + (f" | {m.description}" if m.description else "")
                + (f" | 兼容维度: {','.join(m.compatible_dimensions)}" if m.compatible_dimensions else "")
            )
        return "\n".join(lines) if lines else "（无候选指标）"

    def dimensions_summary(self, names: Optional[List[str]] = None) -> str:
        """
        供 Prompt 使用的维度摘要，返回格式化字符串。
        names: 若提供则只包含这些 name；否则输出全部维度。
        """
        if names is not None:
            pool = [self._dimensions[n] for n in names if n in self._dimensions]
        else:
            pool = list(self._dimensions.values())

        lines = []
        for d in pool:
            syns = "、".join(d.synonyms) if d.synonyms else ""
            syns_str = f"（同义：{syns}）" if syns else ""
            grain_str = f" | 粒度:{d.grain}" if d.grain else ""
            lines.append(f"  - {d.name}：{d.label}{syns_str}{grain_str}")
        return "\n".join(lines) if lines else "（无候选维度）"

    def business_summary(self) -> str:
        """供意图识别使用的业务域摘要，返回格式化字符串。"""
        lines = []
        for b in self._businesses.values():
            qs = "；".join(b.typical_questions[:3]) if b.typical_questions else ""
            qs_str = f" | 典型问题: {qs}" if qs else ""
            lines.append(f"  - {b.name}：{b.label}{qs_str}")
        return "\n".join(lines) if lines else ""

    # ── 导出 & 统计 ───────────────────────────────────────────────────────────

    def dump_yaml(self) -> str:
        """
        从当前内存（即 DB 状态）导出标准 YAML 字符串。
        格式与 catalog_yaml/*.yaml 完全兼容，可直接用于 reload。
        """
        entities = []
        for e in self._entities.values():
            obj: dict = {"name": e.name, "label": e.label}
            if e.description:       obj["description"] = e.description
            if e.primary_table:     obj["primary_table"] = e.primary_table
            if e.primary_key:       obj["primary_key"] = e.primary_key
            if e.display_key:       obj["display_key"] = e.display_key
            if e.searchable_fields: obj["searchable_fields"] = e.searchable_fields
            if e.synonyms:          obj["synonyms"] = e.synonyms
            if e.tags:              obj["tags"] = e.tags
            entities.append(obj)

        dimensions = []
        for d in self._dimensions.values():
            obj = {"name": d.name, "label": d.label, "dim_type": d.dim_type}
            if d.description:   obj["description"] = d.description
            if d.entity:        obj["entity"] = d.entity
            if d.grain:         obj["grain"] = d.grain
            if d.expression:    obj["expression"] = d.expression
            if d.alias and d.alias != d.name:
                                obj["alias"] = d.alias
            if d.join:
                obj["join"] = {
                    "table":     d.join.table,
                    "alias":     d.join.alias,
                    "join_type": d.join.join_type,
                    "on":        d.join.on,
                }
            if d.select_fields: obj["select_fields"] = d.select_fields
            if d.synonyms:      obj["synonyms"] = d.synonyms
            if d.tags:          obj["tags"] = d.tags
            dimensions.append(obj)

        metrics = []
        for m in self._metrics.values():
            obj = {"name": m.name, "label": m.label, "metric_type": m.metric_type}
            if m.description:       obj["description"] = m.description
            if m.complexity != "normal":
                                    obj["complexity"] = m.complexity
            if m.expression:        obj["expression"] = m.expression
            if m.primary_table:
                obj["primary_source"] = {
                    "table": m.primary_table,
                    "alias": m.primary_alias,
                }
            if m.extra_joins:
                obj["extra_joins"] = [
                    {"table": j.table, "alias": j.alias,
                     "join_type": j.join_type, "on": j.on}
                    for j in m.extra_joins
                ]
            if m.time_column:       obj["time_column"] = m.time_column
            if m.numerator_expr:    obj["numerator_expr"] = m.numerator_expr
            if m.denominator_expr:  obj["denominator_expr"] = m.denominator_expr
            if m.output_format and m.output_format != "number":
                                    obj["output_format"] = m.output_format
            if m.unit:              obj["unit"] = m.unit
            if m.compatible_dimensions:
                                    obj["compatible_dimensions"] = m.compatible_dimensions
            if m.synonyms:          obj["synonyms"] = m.synonyms
            if m.tags:              obj["tags"] = m.tags
            metrics.append(obj)

        business = []
        for b in self._businesses.values():
            obj = {"name": b.name, "label": b.label}
            if b.description:           obj["description"] = b.description
            if b.related_metrics:       obj["related_metrics"] = b.related_metrics
            if b.related_dimensions:    obj["related_dimensions"] = b.related_dimensions
            if b.typical_questions:     obj["typical_questions"] = b.typical_questions
            if b.default_dimensions:    obj["default_dimensions"] = b.default_dimensions
            if b.default_sort:          obj["default_sort"] = b.default_sort
            if b.synonyms:              obj["synonyms"] = b.synonyms
            if b.tags:                  obj["tags"] = b.tags
            business.append(obj)

        data = {
            "version": "1.0",
            "db_name": self._db_name,
            "entities":   entities,
            "dimensions": dimensions,
            "metrics":    metrics,
            "business":   business,
        }
        return yaml.dump(data, allow_unicode=True, default_flow_style=False, sort_keys=False)

    def import_yaml(self, yaml_str: str, save_to_db: bool = True) -> dict:
        """
        从 YAML 字符串导入语义定义（DB 为主，YAML 为导入格式）。
        save_to_db=True 时写入 semantic_store，同时刷新同义词索引。
        返回导入统计。
        """
        try:
            raw = yaml.safe_load(yaml_str) or {}
        except Exception as e:
            raise ValueError(f"YAML 解析失败: {e}")
        self._entities, self._dimensions, self._metrics, self._businesses = (
            _parse_catalog(raw)
        )
        if save_to_db:
            self._save_to_db()
        self._build_synonym_index()
        stats = self.stats()
        logger.info("[SemanticCatalog] import_yaml 完成: %s", stats)
        return stats

    def upsert_metric(self, data: dict) -> None:
        """
        单条指标 upsert：更新内存 + DB。
        data 格式同 YAML metric 条目。
        """
        extra_joins = [_parse_join(j) for j in data.get("extra_joins", [])]
        ps = data.get("primary_source")
        m = MetricDef(
            name=data["name"],
            label=data["label"],
            description=data.get("description", ""),
            metric_type=data.get("metric_type", "simple"),
            complexity=data.get("complexity", "normal"),
            expression=data.get("expression", ""),
            primary_source=ps,
            extra_joins=extra_joins,
            time_column=data.get("time_column", ""),
            numerator_expr=data.get("numerator_expr", ""),
            denominator_expr=data.get("denominator_expr", ""),
            output_format=data.get("output_format", "number"),
            unit=data.get("unit", ""),
            compatible_dimensions=data.get("compatible_dimensions", []),
            synonyms=data.get("synonyms", []),
            tags=data.get("tags", []),
        )
        self._metrics[m.name] = m
        # 写入 DB（用 DELETE + INSERT 实现 upsert）
        try:
            self._sem.execute_write(
                "DELETE FROM semantic_store.vanna_semantic_metrics WHERE name = %s AND (db_name = %s OR IFNULL(db_name,'') = '')",
                (m.name, self._db_name),
            )
        except Exception as ex:
            logger.warning("[SemanticCatalog] upsert_metric DELETE 失败: %s", ex)

        extra_joins_json = json.dumps(
            [{"table": j.table, "alias": j.alias, "join_type": j.join_type, "on": j.on}
             for j in m.extra_joins], ensure_ascii=False
        )
        try:
            self._sem.execute_write(
                "INSERT INTO semantic_store.vanna_semantic_metrics "
                "(metric_id, name, label, description, metric_type, complexity, "
                "expression, primary_table, primary_alias, extra_joins_json, "
                "time_column, numerator_expr, denominator_expr, output_format, unit, "
                "compatible_dimensions_json, synonyms, tags, db_name) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    m.node_id, m.name, m.label, m.description or "",
                    m.metric_type, m.complexity, m.expression or "",
                    m.primary_table, m.primary_alias, extra_joins_json,
                    m.time_column or "", m.numerator_expr or "", m.denominator_expr or "",
                    m.output_format, m.unit or "",
                    json.dumps(m.compatible_dimensions, ensure_ascii=False),
                    ",".join(m.synonyms), ",".join(m.tags), self._db_name,
                ),
            )
        except Exception as ex:
            logger.warning("[SemanticCatalog] upsert_metric INSERT 失败: %s", ex)
        self._build_synonym_index()

    def upsert_dimension(self, data: dict) -> None:
        """
        单条维度 upsert：更新内存 + DB。
        data 格式同 YAML dimension 条目。
        """
        join_raw = data.get("join")
        d = DimensionDef(
            name=data["name"],
            label=data["label"],
            description=data.get("description", ""),
            dim_type=data.get("dim_type", "attribute"),
            entity=data.get("entity"),
            grain=data.get("grain"),
            expression=data.get("expression", ""),
            alias=data.get("alias", data["name"]),
            join=_parse_join(join_raw) if join_raw else None,
            select_fields=data.get("select_fields", []),
            synonyms=data.get("synonyms", []),
            tags=data.get("tags", []),
        )
        self._dimensions[d.name] = d
        try:
            self._sem.execute_write(
                "DELETE FROM semantic_store.vanna_semantic_dimensions WHERE name = %s AND (db_name = %s OR IFNULL(db_name,'') = '')",
                (d.name, self._db_name),
            )
        except Exception as ex:
            logger.warning("[SemanticCatalog] upsert_dimension DELETE 失败: %s", ex)

        j = d.join
        try:
            self._sem.execute_write(
                "INSERT INTO semantic_store.vanna_semantic_dimensions "
                "(dim_id, name, label, description, dim_type, entity, grain, "
                "expression, alias, join_table, join_alias, join_type, join_on, "
                "select_fields_json, synonyms, tags, db_name) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    d.node_id, d.name, d.label, d.description or "",
                    d.dim_type, d.entity or "", d.grain or "",
                    d.expression or "", d.alias or d.name,
                    j.table if j else "", j.alias if j else "",
                    j.join_type if j else "LEFT JOIN", j.on if j else "",
                    json.dumps(d.select_fields, ensure_ascii=False),
                    ",".join(d.synonyms), ",".join(d.tags), self._db_name,
                ),
            )
        except Exception as ex:
            logger.warning("[SemanticCatalog] upsert_dimension INSERT 失败: %s", ex)
        self._build_synonym_index()

    def delete_node(self, node_type: str, name: str) -> bool:
        """
        删除单个节点（指标/维度/实体/业务域），更新内存 + DB。
        node_type: metric | dimension | entity | business
        """
        table_map = {
            "metric": ("vanna_semantic_metrics", "metric_id", self._metrics),
            "dimension": ("vanna_semantic_dimensions", "dim_id", self._dimensions),
            "entity": ("vanna_semantic_entities", "entity_id", self._entities),
            "business": ("vanna_semantic_businesses", "biz_id", self._businesses),
        }
        if node_type not in table_map:
            return False
        table, _, mem_dict = table_map[node_type]
        col_map = {"metric": "name", "dimension": "name",
                   "entity": "name", "business": "name"}
        if name not in mem_dict:
            return False
        mem_dict.pop(name)
        try:
            self._sem.execute_write(
                f"DELETE FROM semantic_store.{table} WHERE name = %s AND (db_name = %s OR IFNULL(db_name,'') = '')",
                (name, self._db_name),
            )
        except Exception as ex:
            logger.warning("[SemanticCatalog] delete_node 失败: %s", ex)
        self._build_synonym_index()
        return True

    def save_yaml_file(self) -> str:
        """
        把当前 DB 状态导出并覆写本地 YAML 文件（backup 用）。
        返回写入的文件路径。
        """
        yaml_str = self.dump_yaml()
        yaml_path = _YAML_DIR / f"{self._db_name}.yaml"
        yaml_path.parent.mkdir(parents=True, exist_ok=True)
        yaml_path.write_text(yaml_str, encoding="utf-8")
        logger.info("[SemanticCatalog] YAML 已写入: %s", yaml_path)
        return str(yaml_path)

    def stats(self) -> dict:
        return {
            "entities": len(self._entities),
            "dimensions": len(self._dimensions),
            "metrics": len(self._metrics),
            "businesses": len(self._businesses),
            "synonym_entries": len(self._synonym_index),
        }


# ─────────────────────────────────────────────────────────────────────────────
# 工具函数
# ─────────────────────────────────────────────────────────────────────────────

def _split_csv(value: Optional[str]) -> List[str]:
    """把逗号分隔字符串拆成列表，过滤空项。"""
    if not value:
        return []
    return [s.strip() for s in value.split(",") if s.strip()]
