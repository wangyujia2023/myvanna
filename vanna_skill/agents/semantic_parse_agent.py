"""
SemanticParseAgent：语义解析 Agent

职责：
  1. 接收 IntentPlan，从 SemanticCatalog 中召回候选指标/维度
  2. 调用 LLM 精确映射：把用户问题中的表述映射到 catalog 中的 metric/dimension name
  3. 输出 SemanticPlan（含 coverage_score，决定后续路由）

路由规则（coverage_score）：
  ≥ 0.8  → 全语义路径（SQLSynthesizer 直接生成 SQL）
  0.5-0.8 → 混合路径（语义 + LLM 辅助生成）
  < 0.5  → LangChain 降级（SemanticPlan 作为额外上下文注入）
"""
from __future__ import annotations

import json
import logging
import re
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from ..semantic.models import FilterCondition, IntentPlan, QuerySpec, SemanticPlan
from ..semantic.time_compiler import enrich_query_spec

if TYPE_CHECKING:
    from ..semantic.catalog import SemanticCatalog
    from ..qwen_client import QwenClient

logger = logging.getLogger(__name__)


class SemanticParseAgent:
    """
    把 IntentPlan 转化为 SemanticPlan。

    Parameters
    ----------
    llm     : QwenClient 或兼容接口
    catalog : SemanticCatalog 单例
    """

    def __init__(
        self,
        llm: "QwenClient",
        catalog: "SemanticCatalog",
    ) -> None:
        self._llm = llm
        self._catalog = catalog

    # ─────────────────────────────────────────────────────────────────────────
    # 公共入口
    # ─────────────────────────────────────────────────────────────────────────

    def run(self, intent: IntentPlan) -> SemanticPlan:
        """
        解析意图，返回 SemanticPlan。
        异常时返回 coverage_score=0 的空计划（触发 LangChain 降级）。
        """
        question = intent.normalized_query or intent.raw_question

        try:
            # Step 1: 本地快速召回候选集
            candidate_metrics = self._recall_metrics(intent)
            candidate_dims = self._recall_dimensions(intent)

            # Step 2: LLM 精确映射
            plan = self._llm_map(question, intent, candidate_metrics, candidate_dims)

            # Step 3: 计算 coverage_score
            plan.coverage_score = self._catalog.coverage_score(
                plan.metrics, plan.dimensions
            )
            plan.intent_plan = intent
            if plan.query_spec is None:
                plan.query_spec = QuerySpec(
                    metrics=list(plan.metrics),
                    dimensions=list(plan.dimensions),
                    filters=list(plan.filters),
                    order_by=list(plan.order_by),
                    limit=plan.limit,
                    unresolved_parts=list(plan.unresolved_parts),
                )
            plan.query_spec = enrich_query_spec(question, intent, plan.query_spec)
            self._normalize_comparison_metrics(plan)
            plan.unresolved_parts = self._normalize_unresolved(
                plan.unresolved_parts,
                plan.query_spec,
            )
            plan.query_spec.unresolved_parts = list(plan.unresolved_parts)
            return plan

        except Exception as exc:
            logger.warning(f"[SemanticParseAgent] 解析失败，降级: {exc}")
            return SemanticPlan(
                coverage_score=0.0,
                unresolved_parts=[question],
                intent_plan=intent,
            )

    def run_with_sql_rag(
        self,
        intent: IntentPlan,
        sql_examples: Optional[List[Dict[str, Any]]] = None,
    ) -> SemanticPlan:
        question = intent.normalized_query or intent.raw_question

        try:
            candidate_metrics = self._recall_metrics(intent)
            candidate_dims = self._recall_dimensions(intent)

            plan = self._llm_map(
                question,
                intent,
                candidate_metrics,
                candidate_dims,
                sql_examples=sql_examples or [],
            )

            plan.coverage_score = self._catalog.coverage_score(
                plan.metrics, plan.dimensions
            )
            plan.intent_plan = intent
            if plan.query_spec is None:
                plan.query_spec = QuerySpec(
                    metrics=list(plan.metrics),
                    dimensions=list(plan.dimensions),
                    filters=list(plan.filters),
                    order_by=list(plan.order_by),
                    limit=plan.limit,
                    unresolved_parts=list(plan.unresolved_parts),
                )
            plan.query_spec = enrich_query_spec(question, intent, plan.query_spec)
            self._normalize_comparison_metrics(plan)
            plan.unresolved_parts = self._normalize_unresolved(
                plan.unresolved_parts,
                plan.query_spec,
            )
            plan.query_spec.unresolved_parts = list(plan.unresolved_parts)
            return plan
        except Exception as exc:
            logger.warning(f"[SemanticParseAgent] 解析失败，降级: {exc}")
            return SemanticPlan(
                coverage_score=0.0,
                unresolved_parts=[question],
                intent_plan=intent,
            )

    # ─────────────────────────────────────────────────────────────────────────
    # 内部：候选召回
    # ─────────────────────────────────────────────────────────────────────────

    def _recall_metrics(self, intent: IntentPlan) -> List[str]:
        """
        从 intent 关键词 + 业务域中召回候选指标 name 列表。
        """
        keywords: List[str] = []
        keywords.extend(intent.entity_hints)
        keywords.extend(intent.action_hints)
        keywords.append(intent.business_domain)
        # 从标准化问句里提取潜在关键词（按空格/逗号分割）
        for word in re.split(r"[\s,，、]+", intent.normalized_query):
            if len(word) >= 2:
                keywords.append(word)

        hits = self._catalog.match_metrics(keywords)
        # 同时追加业务域默认指标（如果 catalog 有 BusinessDef）
        biz = self._catalog.get_business(intent.business_domain)
        if biz:
            for m in biz.related_metrics:
                if m not in hits:
                    hits.append(m)

        return hits[:20]  # 候选上限 20

    def _recall_dimensions(self, intent: IntentPlan) -> List[str]:
        """
        从 intent 关键词中召回候选维度 name 列表。
        """
        keywords: List[str] = []
        keywords.extend(intent.entity_hints)
        # 时间粒度关键词
        if intent.time_hint:
            if "天" in intent.time_hint or "日" in intent.time_hint:
                keywords.append("time_day")
            elif "-" in intent.time_hint and len(intent.time_hint) == 7:
                keywords.append("time_month")
            elif len(intent.time_hint) == 4:
                keywords.append("time_year")
        for word in re.split(r"[\s,，、]+", intent.normalized_query):
            if len(word) >= 2:
                keywords.append(word)

        hits = self._catalog.match_dimensions(keywords)
        biz = self._catalog.get_business(intent.business_domain)
        if biz:
            for d in biz.related_dimensions:
                if d not in hits:
                    hits.append(d)

        return hits[:15]

    # ─────────────────────────────────────────────────────────────────────────
    # 内部：LLM 精确映射
    # ─────────────────────────────────────────────────────────────────────────

    def _llm_map(
        self,
        question: str,
        intent: IntentPlan,
        candidate_metrics: List[str],
        candidate_dims: List[str],
        sql_examples: Optional[List[Dict[str, Any]]] = None,
    ) -> SemanticPlan:
        """调用 LLM 从候选集中精确选出 metrics / dimensions / filters。"""
        logger.info(
            "[SemanticParseAgent] LLM映射 | question=%r | candidate_metrics=%s | candidate_dims=%s",
            question[:80], candidate_metrics, candidate_dims,
        )

        prompt = self._build_prompt(
            question,
            intent,
            candidate_metrics,
            candidate_dims,
            sql_examples=sql_examples or [],
        )
        logger.debug("[SemanticParseAgent] prompt=\n%s", prompt)

        try:
            raw = self._llm.generate(prompt, temperature=0.0).strip()
            logger.debug("[SemanticParseAgent] LLM raw=\n%s", raw)
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw).strip()
            data: Dict[str, Any] = json.loads(raw)
            logger.info("[SemanticParseAgent] LLM解析结果=%s", data)
        except Exception as exc:
            logger.warning(f"[SemanticParseAgent] LLM/JSON 失败: {exc}")
            # 降级：把召回的全部候选作为答案
            return SemanticPlan(
                metrics=candidate_metrics[:5],
                dimensions=candidate_dims[:3],
                unresolved_parts=[question],
            )

        return self._build_plan(data, candidate_metrics, candidate_dims, question)

    def _build_prompt(
        self,
        question: str,
        intent: IntentPlan,
        candidate_metrics: List[str],
        candidate_dims: List[str],
        sql_examples: Optional[List[Dict[str, Any]]] = None,
    ) -> str:
        metrics_detail = self._catalog.metrics_summary(candidate_metrics)
        dims_detail = self._catalog.dimensions_summary(candidate_dims)
        time_note = f"\n已识别时间范围：{intent.time_hint}" if intent.time_hint else ""
        sql_example_note = ""
        if sql_examples:
            lines = []
            for idx, item in enumerate(sql_examples[:3], start=1):
                lines.append(
                    f"{idx}. 问题：{item.get('raw_question') or item.get('question', '')}\n"
                    f"   模板：{item.get('canonical_question', '')}\n"
                    f"   SQL：{item.get('sql_text') or item.get('content', '')}"
                )
            sql_example_note = "\n\n【历史正确 SQL 示例】\n" + "\n".join(lines)

        return (
            "你是一个语义解析模型，负责把用户问题映射到已知指标和维度。\n\n"
            f"【候选指标】\n{metrics_detail}\n\n"
            f"【候选维度】\n{dims_detail}\n"
            f"{time_note}\n\n"
            f"{sql_example_note}\n"
            "请从候选集中选出最合适的指标和维度，输出 JSON，字段说明：\n"
            "  metrics    : 选用的指标 name 列表（严格使用上面给出的 name）\n"
            "  dimensions : 选用的维度 name 列表（严格使用上面给出的 name）\n"
            "  filters    : 过滤条件列表，每项 {column, operator, value}，"
            "               operator 可选 = / >= / <= / BETWEEN / IN / LIKE\n"
            "               BETWEEN 时额外加 value2 字段\n"
            "               column 优先使用候选维度 name，不要发明 time/date/日期 这类泛化列名；\n"
            "               时间范围、同比、环比不要放进 filters，由系统编译器统一处理。\n"
            "  order_by   : 排序列表，每项 {field, direction}，direction=ASC/DESC\n"
            "  limit      : 返回行数，默认 20\n"
            "  unresolved : 无法映射的片段列表（没有则空数组）\n"
            "只返回 JSON，不要解释。\n\n"
            f"用户问题：{question}"
        )

    def _build_plan(
        self,
        data: Dict[str, Any],
        candidate_metrics: List[str],
        candidate_dims: List[str],
        question: str,
    ) -> SemanticPlan:
        """把 LLM 返回 dict 转成 SemanticPlan，做安全校验。"""
        # 指标：只保留 catalog 中存在的名称（防止 LLM 幻觉）
        raw_metrics: List[str] = data.get("metrics", [])
        metrics = []
        resolved_metric_map = {}
        for m in raw_metrics:
            resolved = self._resolve_metric_name(m)
            if resolved and resolved not in metrics:
                metrics.append(resolved)
            resolved_metric_map[m] = resolved
        hallucinated_metrics = [m for m in raw_metrics if not resolved_metric_map.get(m)]
        if hallucinated_metrics:
            logger.warning(
                "[SemanticParseAgent] LLM返回了不在catalog中的指标（已过滤）: %s",
                hallucinated_metrics,
            )

        # 维度：只保留 catalog 中存在的名称（防止 LLM 幻觉）
        raw_dims: List[str] = data.get("dimensions", [])
        dimensions = []
        resolved_dim_map = {}
        for d in raw_dims:
            resolved = self._resolve_dimension_name(d)
            if resolved and resolved not in dimensions:
                dimensions.append(resolved)
            resolved_dim_map[d] = resolved
        hallucinated_dims = [d for d in raw_dims if not resolved_dim_map.get(d)]
        if hallucinated_dims:
            logger.warning(
                "[SemanticParseAgent] LLM返回了不在catalog中的维度（已过滤）: %s",
                hallucinated_dims,
            )

        # filters
        filters: List[FilterCondition] = []
        metric_time_column = ""
        for m_name in metrics:
            metric = self._catalog.get_metric(m_name)
            if metric and metric.time_column:
                metric_time_column = metric.time_column
                break

        for f in data.get("filters", []):
            if isinstance(f, dict) and "column" in f:
                try:
                    column = str(f["column"])
                    if (
                        metric_time_column
                        and column.strip().lower() in {"time", "date", "日期", "时间"}
                    ):
                        logger.info(
                            "[SemanticParseAgent] 将泛化时间字段 %r 规范化为 %r",
                            column,
                            metric_time_column,
                        )
                        column = metric_time_column
                    filters.append(FilterCondition(
                        column=column,
                        operator=str(f.get("operator", "=")),
                        value=f.get("value"),
                        value2=f.get("value2"),
                    ))
                except Exception:
                    pass

        # order_by
        order_by: List[Dict] = []
        for o in data.get("order_by", []):
            if isinstance(o, dict) and "field" in o:
                order_by.append({
                    "field": str(o["field"]),
                    "direction": str(o.get("direction", "DESC")).upper(),
                })

        # limit
        try:
            limit = int(data.get("limit", 20))
        except Exception:
            limit = 20

        # unresolved
        unresolved = [str(u) for u in data.get("unresolved", [])]
        if not metrics and not dimensions:
            unresolved.append(question)

        query_spec = QuerySpec(
            metrics=list(metrics),
            dimensions=list(dimensions),
            filters=list(filters),
            order_by=list(order_by),
            limit=limit,
            unresolved_parts=list(unresolved),
        )

        logger.info(
            "[SemanticParseAgent] SemanticPlan结果 | metrics=%s | dims=%s | "
            "filters=%d | unresolved=%s",
            metrics, dimensions, len(filters), unresolved,
        )

        return SemanticPlan(
            metrics=metrics,
            dimensions=dimensions,
            filters=filters,
            order_by=order_by,
            limit=limit,
            unresolved_parts=unresolved,
            query_spec=query_spec,
        )

    def _resolve_metric_name(self, raw_name: str) -> str:
        raw = (raw_name or "").strip()
        if not raw:
            return ""
        if self._catalog.get_metric(raw):
            return raw
        raw_l = raw.lower()
        for name, metric in self._catalog._metrics.items():
            candidates = [name, metric.label] + list(metric.synonyms)
            if any(raw_l == str(c).strip().lower() for c in candidates if c):
                return name
        return ""

    def _resolve_dimension_name(self, raw_name: str) -> str:
        raw = (raw_name or "").strip()
        if not raw:
            return ""
        if self._catalog.get_dimension(raw):
            return raw
        raw_l = raw.lower()
        for name, dim in self._catalog._dimensions.items():
            candidates = [name, dim.label] + list(dim.synonyms)
            if any(raw_l == str(c).strip().lower() for c in candidates if c):
                return name
        return ""

    def _normalize_unresolved(
        self, unresolved: List[str], query_spec: QuerySpec
    ) -> List[str]:
        if not unresolved:
            return []
        normalized: List[str] = []
        for item in unresolved:
            token = (item or "").strip()
            if not token:
                continue
            if query_spec.time_scope and any(k in token for k in ["昨天", "昨日", "今天", "今日", "本月", "上月", "今年", "去年", "季度", "月份", "4月", "时间"]):
                continue
            if query_spec.comparison and query_spec.comparison.enabled and any(k in token for k in ["环比", "同比", "同期", "增长", "增长率"]):
                continue
            normalized.append(token)
        return normalized

    def _normalize_comparison_metrics(self, plan: SemanticPlan) -> None:
        spec = plan.query_spec
        if not spec or not spec.comparison or not spec.comparison.enabled:
            return
        if not plan.metrics:
            return

        normalized: List[str] = []
        remapped: Dict[str, str] = {}
        dropped: List[str] = []

        for name in plan.metrics:
            base_name = self._resolve_base_metric_for_comparison(name)
            if base_name and base_name != name:
                remapped[name] = base_name
                name = base_name
            if name not in normalized:
                normalized.append(name)

        plan.metrics = normalized
        spec.metrics = list(normalized)

        if len(plan.metrics) <= 1:
            if remapped:
                logger.info(
                    "[SemanticParseAgent] comparison 场景将派生指标回落到基础指标=%s",
                    remapped,
                )
            return

        kept: List[str] = []
        for name in normalized:
            metric = self._catalog.get_metric(name)
            label = metric.label if metric else name
            key = f"{name} {label}".lower()
            if any(token in key for token in ["growth", "rate", "ratio", "yoy", "mom", "wow", "增", "环比", "同比", "增长"]):
                dropped.append(name)
                continue
            kept.append(name)
        if kept and dropped:
            logger.info(
                "[SemanticParseAgent] comparison 场景归一化指标 | remapped=%s | dropped=%s | kept=%s",
                remapped,
                dropped,
                kept,
            )
            plan.metrics = kept
            spec.metrics = list(kept)
        elif remapped:
            logger.info(
                "[SemanticParseAgent] comparison 场景将派生指标回落到基础指标=%s",
                remapped,
            )

    def _resolve_base_metric_for_comparison(self, metric_name: str) -> str:
        metric = self._catalog.get_metric(metric_name)
        if not metric:
            return metric_name

        key = f"{metric.name} {metric.label}".lower()
        if not any(token in key for token in ["growth", "rate", "ratio", "yoy", "mom", "wow", "增", "环比", "同比", "增长"]):
            return metric_name

        direct_candidates = self._candidate_metric_names(metric.name)
        direct_candidates.extend(self._candidate_metric_names(metric.label))

        for candidate in direct_candidates:
            if candidate != metric_name and self._catalog.get_metric(candidate):
                return candidate

        normalized_metric_name = self._normalized_metric_key(metric.name)
        normalized_label = self._normalized_metric_key(metric.label)

        for candidate_name, candidate_metric in self._catalog._metrics.items():
            if candidate_name == metric_name:
                continue
            candidate_key = self._normalized_metric_key(candidate_name)
            candidate_label = self._normalized_metric_key(candidate_metric.label)
            if not candidate_key and not candidate_label:
                continue
            if normalized_metric_name and candidate_key and normalized_metric_name == candidate_key:
                return candidate_name
            if normalized_label and candidate_label and normalized_label == candidate_label:
                return candidate_name
            if normalized_label and candidate_key and normalized_label == candidate_key:
                return candidate_name
            if normalized_metric_name and candidate_label and normalized_metric_name == candidate_label:
                return candidate_name

        return metric_name

    def _candidate_metric_names(self, value: str) -> List[str]:
        raw = (value or "").strip()
        if not raw:
            return []

        patterns = [
            r"(_?(growth|rate|ratio|yoy|mom|wow))+$",
            r"(同比增长率|同比增长|同比|环比增长率|环比增长|环比|增长率|增长|比率|占比|率)$",
        ]
        candidates: List[str] = []
        current = raw
        for pattern in patterns:
            updated = re.sub(pattern, "", current, flags=re.IGNORECASE).strip(" _-")
            if updated and updated != current:
                candidates.append(updated)
                current = updated
        return candidates

    def _normalized_metric_key(self, value: str) -> str:
        cleaned = (value or "").strip().lower()
        if not cleaned:
            return ""
        cleaned = re.sub(r"(_?(growth|rate|ratio|yoy|mom|wow))+$", "", cleaned)
        cleaned = re.sub(
            r"(同比增长率|同比增长|同比|环比增长率|环比增长|环比|增长率|增长|比率|占比|率)$",
            "",
            cleaned,
        )
        cleaned = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", "", cleaned)
        return cleaned
