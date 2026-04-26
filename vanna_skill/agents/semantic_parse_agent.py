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

from ..semantic.models import FilterCondition, IntentPlan, SemanticPlan

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
    ) -> SemanticPlan:
        """调用 LLM 从候选集中精确选出 metrics / dimensions / filters。"""
        logger.info(
            "[SemanticParseAgent] LLM映射 | question=%r | candidate_metrics=%s | candidate_dims=%s",
            question[:80], candidate_metrics, candidate_dims,
        )

        prompt = self._build_prompt(question, intent, candidate_metrics, candidate_dims)
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
    ) -> str:
        metrics_detail = self._catalog.metrics_summary(candidate_metrics)
        dims_detail = self._catalog.dimensions_summary(candidate_dims)
        time_note = f"\n已识别时间范围：{intent.time_hint}" if intent.time_hint else ""

        return (
            "你是一个语义解析模型，负责把用户问题映射到已知指标和维度。\n\n"
            f"【候选指标】\n{metrics_detail}\n\n"
            f"【候选维度】\n{dims_detail}\n"
            f"{time_note}\n\n"
            "请从候选集中选出最合适的指标和维度，输出 JSON，字段说明：\n"
            "  metrics    : 选用的指标 name 列表（严格使用上面给出的 name）\n"
            "  dimensions : 选用的维度 name 列表（严格使用上面给出的 name）\n"
            "  filters    : 过滤条件列表，每项 {column, operator, value}，"
            "               operator 可选 = / >= / <= / BETWEEN / IN / LIKE\n"
            "               BETWEEN 时额外加 value2 字段\n"
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
        metrics = [m for m in raw_metrics if self._catalog.get_metric(m) is not None]
        hallucinated_metrics = [m for m in raw_metrics if m not in metrics]
        if hallucinated_metrics:
            logger.warning(
                "[SemanticParseAgent] LLM返回了不在catalog中的指标（已过滤）: %s",
                hallucinated_metrics,
            )

        # 维度：只保留 catalog 中存在的名称（防止 LLM 幻觉）
        raw_dims: List[str] = data.get("dimensions", [])
        dimensions = [d for d in raw_dims if self._catalog.get_dimension(d) is not None]
        hallucinated_dims = [d for d in raw_dims if d not in dimensions]
        if hallucinated_dims:
            logger.warning(
                "[SemanticParseAgent] LLM返回了不在catalog中的维度（已过滤）: %s",
                hallucinated_dims,
            )

        # filters
        filters: List[FilterCondition] = []
        for f in data.get("filters", []):
            if isinstance(f, dict) and "column" in f:
                try:
                    filters.append(FilterCondition(
                        column=str(f["column"]),
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
        )
