"""
Vanna Skill 管理后台 - Streamlit 多页签界面
运行: streamlit run ui/admin.py

页签：
  📊 仪表盘    - 统计概览 + Gemini 健康状态
  📚 训练数据  - 增删查、批量导入、质量评分
  🗄️ 元数据   - 表/列浏览、同步、一键 DDL 训练
  🔗 血缘      - 表级血缘图 + 影响分析
  🔍 调试      - 输入问题查看完整调用链
  📋 调用日志  - 历史请求追踪记录
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time

import pandas as pd
import streamlit as st

# ── Streamlit 页面配置（必须是第一个 st 调用）──────────────────────────────
st.set_page_config(
    page_title="Vanna Skill 管理台",
    page_icon="🧠",
    layout="wide",
    initial_sidebar_state="expanded",
)

from vanna_skill import (
    DorisVanna, DorisClient, MetadataManager,
    LineageManager, AuditMiner, tracer, load_config,
)

# ── 配置 ────────────────────────────────────────────────────────────────────
CONFIG = load_config()


# ── 单例缓存（避免每次 rerun 重新创建连接）────────────────────────────────────
@st.cache_resource
def get_vanna():
    return DorisVanna(CONFIG)

@st.cache_resource
def get_biz_db():
    return DorisClient(CONFIG["host"], CONFIG["port"],
                       CONFIG["user"], CONFIG["password"],
                       CONFIG["database"])

@st.cache_resource
def get_vec_db():
    return DorisClient(CONFIG["host"], CONFIG["port"],
                       CONFIG["user"], CONFIG["password"],
                       "vanna_store")

@st.cache_resource
def get_metadata():
    return MetadataManager(get_biz_db(), CONFIG["database"])

@st.cache_resource
def get_lineage():
    return LineageManager(biz_doris=get_biz_db(), vec_doris=get_vec_db())

@st.cache_resource
def get_miner():
    return AuditMiner(get_biz_db(), get_vanna())


# ────────────────────────────────────────────────────────────────────────────
# 侧边栏
# ────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.image("https://avatars.githubusercontent.com/u/106217094?s=48", width=48)
    st.markdown("## 🧠 Vanna Skill")
    st.caption(f"Doris: {CONFIG['host']}:{CONFIG['port']} | DB: {CONFIG['database']}")
    st.divider()

    page = st.radio("导航", [
        "📊 仪表盘",
        "📚 训练数据",
        "🗄️ 元数据管理",
        "🔗 血缘分析",
        "🔍 调试控制台",
        "📋 调用日志",
    ], label_visibility="collapsed")

    st.divider()
    # 快速连通性检测
    if st.button("🔌 测试连接", use_container_width=True):
        try:
            ok = get_biz_db().test()
            if ok:
                st.success("Doris 连接正常")
            else:
                st.error("Doris 连接失败")
        except Exception as e:
            st.error(f"连接异常: {e}")


# ════════════════════════════════════════════════════════════════════════════
# 📊 仪表盘
# ════════════════════════════════════════════════════════════════════════════
if page == "📊 仪表盘":
    st.title("📊 Vanna Skill 仪表盘")

    # 统计卡片
    col1, col2, col3, col4 = st.columns(4)
    try:
        vn = get_vanna()
        df = vn.get_training_data()
        total = len(df)
        by_type = df["content_type"].value_counts().to_dict()
        t_stats = tracer.stats()
        gem_stats = vn.gemini_stats
    except Exception as e:
        st.error(f"获取统计失败: {e}")
        df = pd.DataFrame()
        total, by_type, t_stats, gem_stats = 0, {}, {}, {}

    col1.metric("向量库总条目", total)
    col2.metric("SQL 问答对", by_type.get("sql", 0))
    col3.metric("DDL 条目", by_type.get("ddl", 0))
    col4.metric("文档条目", by_type.get("doc", 0))

    st.divider()

    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("🤖 Gemini API 状态")
        c1, c2, c3 = st.columns(3)
        c1.metric("Embedding 调用", gem_stats.get("embed_calls", 0))
        c2.metric("缓存命中率", gem_stats.get("embed_cache_hit_rate", "N/A"))
        c3.metric("缓存大小", gem_stats.get("cache_size", 0))
        cc1, cc2 = st.columns(2)
        cc1.metric("LLM 调用次数", gem_stats.get("llm_calls", 0))
        cc2.metric("重试次数", gem_stats.get("retries", 0))

    with col_b:
        st.subheader("⚡ 请求统计")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("总请求", t_stats.get("total", 0))
        c2.metric("成功", t_stats.get("ok", 0))
        c3.metric("失败", t_stats.get("error", 0))
        c4.metric("成功率", t_stats.get("success_rate", "N/A"))
        cc1, cc2 = st.columns(2)
        cc1.metric("平均耗时", f"{t_stats.get('avg_ms', 0):.0f}ms")
        cc2.metric("P95 耗时", f"{t_stats.get('p95_ms', 0):.0f}ms")

    # 训练数据来源分布
    if not df.empty and "source" in df.columns:
        st.divider()
        st.subheader("📈 训练数据来源分布")
        src_cnt = df["source"].value_counts().reset_index()
        src_cnt.columns = ["来源", "数量"]
        st.bar_chart(src_cnt.set_index("来源"))

    # 最近调用
    st.divider()
    st.subheader("🕐 最近请求")
    recent = tracer.recent(10)
    if recent:
        for t in recent:
            status_icon = "✅" if t.status == "ok" else ("❌" if t.status == "error" else "⏳")
            st.markdown(
                f"`{t.trace_id}` {status_icon} **{t.question[:50]}**  "
                f"— {t.total_ms:.0f}ms @ {t.created_at}"
            )
    else:
        st.info("暂无请求记录，请前往「调试控制台」发起测试")


# ════════════════════════════════════════════════════════════════════════════
# 📚 训练数据管理
# ════════════════════════════════════════════════════════════════════════════
elif page == "📚 训练数据":
    st.title("📚 训练数据管理")

    import requests as _req
    API_BASE = "http://localhost:8765"

    vn = get_vanna()
    (tab_browse, tab_add, tab_audit, tab_schema,
     tab_semantic, tab_scan, tab_sync) = st.tabs([
        "浏览/删除", "手动添加", "🔄 挖掘 audit_log", "📥 同步元数据",
        "🧬 语义配置", "🤖 自动扫描", "📤 同步管理",
    ])

    # ── 浏览/删除 ────────────────────────────────────────────────────────────
    with tab_browse:
        if st.button("🔄 刷新"):
            st.cache_data.clear()

        try:
            df = vn.get_training_data()
        except Exception as e:
            st.error(f"获取失败: {e}")
            df = pd.DataFrame()

        if not df.empty:
            # 过滤
            col_f1, col_f2 = st.columns(2)
            with col_f1:
                type_filter = st.multiselect(
                    "类型筛选", ["sql", "ddl", "doc"],
                    default=["sql", "ddl", "doc"]
                )
            with col_f2:
                src_filter = st.multiselect(
                    "来源筛选",
                    df["source"].dropna().unique().tolist() if "source" in df else [],
                )

            display_df = df[df["content_type"].isin(type_filter)]
            if src_filter:
                display_df = display_df[display_df["source"].isin(src_filter)]

            st.caption(f"共 {len(display_df)} 条")
            st.dataframe(display_df, use_container_width=True, height=400)

            # 删除
            st.divider()
            del_id = st.text_input("输入要删除的 ID")
            if st.button("🗑️ 删除", type="secondary"):
                if del_id:
                    try:
                        ok = vn.remove_training_data(del_id)
                        st.success(f"已删除 ID={del_id}") if ok else st.warning("未找到该 ID")
                    except Exception as e:
                        st.error(f"删除失败: {e}")
                else:
                    st.warning("请输入 ID")
        else:
            st.info("向量库暂无数据，请先同步元数据或添加训练数据")

    # ── 手动添加 ─────────────────────────────────────────────────────────────
    with tab_add:
        add_type = st.radio("类型", ["question-sql 对", "DDL", "业务文档"],
                            horizontal=True)

        if add_type == "question-sql 对":
            q = st.text_area("自然语言问题", height=80,
                             placeholder="上个月各大区的销售额分别是多少？")
            sql = st.text_area("对应 SQL", height=150,
                               placeholder="SELECT region, SUM(pay_amount)...")
            if st.button("✅ 添加 Q&A 对", type="primary"):
                if q and sql:
                    with st.spinner("正在生成 Embedding 并写入..."):
                        try:
                            vn.add_question_sql(q, sql, source="manual")
                            st.success("添加成功！")
                        except Exception as e:
                            st.error(f"添加失败: {e}")
                else:
                    st.warning("问题和 SQL 均不能为空")

        elif add_type == "DDL":
            ddl = st.text_area("DDL 定义", height=200,
                               placeholder="CREATE TABLE ...")
            if st.button("✅ 添加 DDL", type="primary"):
                if ddl:
                    with st.spinner("正在生成 Embedding..."):
                        try:
                            vn.add_ddl(ddl)
                            st.success("DDL 添加成功！")
                        except Exception as e:
                            st.error(f"失败: {e}")
                else:
                    st.warning("DDL 不能为空")

        else:
            doc = st.text_area("业务文档/说明", height=200,
                               placeholder="dws_user_trade_daily 表记录每个用户每天的交易聚合数据...")
            if st.button("✅ 添加文档", type="primary"):
                if doc:
                    with st.spinner("正在生成 Embedding..."):
                        try:
                            vn.add_documentation(doc)
                            st.success("文档添加成功！")
                        except Exception as e:
                            st.error(f"失败: {e}")
                else:
                    st.warning("文档不能为空")

    # ── audit_log 挖掘 ────────────────────────────────────────────────────────
    with tab_audit:
        st.subheader("🔄 从 audit_log 挖掘历史 SQL")
        col1, col2 = st.columns(2)
        with col1:
            max_ms = st.slider("最大查询耗时 (ms)", 100, 60000, 30000, 1000)
            limit = st.number_input("最多获取条数", 100, 5000, 1000, 100)
        with col2:
            auto_q = st.checkbox("用 Gemini 自动生成问题描述", value=True)
            st.info("开启后每条 SQL 会调用一次 Gemini，可能较慢")

        col_prev, col_run = st.columns(2)
        with col_prev:
            if st.button("👁️ 预览可挖掘 SQL"):
                miner = get_miner()
                with st.spinner("查询中..."):
                    preview = miner.preview(limit=10)
                if preview and "error" not in preview[0]:
                    st.dataframe(pd.DataFrame(preview), use_container_width=True)
                else:
                    st.warning(f"查询失败或无数据: {preview}")

        with col_run:
            if st.button("🚀 开始挖掘", type="primary"):
                miner = get_miner()
                with st.spinner("挖掘中，请稍候..."):
                    result = miner.mine(
                        max_query_time_ms=max_ms,
                        limit=int(limit),
                        auto_generate_question=auto_q,
                    )
                st.json(result)

    # ── 同步元数据 ────────────────────────────────────────────────────────────
    with tab_schema:
        st.subheader("📥 从 information_schema 同步元数据")
        st.info(f"当前业务库：**{CONFIG['database']}**")
        if st.button("🚀 一键同步元数据（DDL + 摘要）", type="primary"):
            meta = get_metadata()
            vn = get_vanna()
            with st.spinner("同步中..."):
                try:
                    tables = meta.sync()
                    progress = st.progress(0)
                    added_ddl, added_doc = 0, 0
                    for i, m in enumerate(tables):
                        vn.add_ddl(m.to_ddl(), source="schema")
                        vn.add_documentation(m.summary(), source="schema")
                        added_ddl += 1
                        added_doc += 1
                        progress.progress((i + 1) / len(tables))
                    st.success(
                        f"同步完成！共 {len(tables)} 张表，"
                        f"写入 {added_ddl} 条 DDL + {added_doc} 条摘要"
                    )
                except Exception as e:
                    st.error(f"同步失败: {e}")

    # ── 🧬 语义配置 ────────────────────────────────────────────────────────────
    with tab_semantic:
        st.subheader("🧬 语义定义管理")
        st.caption("指标、维度、实体、业务域均存储在 **semantic_store** 数据库（DB 是主），通过此页面增删改查。")

        # 加载 catalog 数据
        try:
            resp = _req.get(f"{API_BASE}/semantic/catalog", timeout=10)
            resp.raise_for_status()
            cat = resp.json()
        except Exception as e:
            st.warning(f"⚠️ 无法连接 API（{e}），请先启动后端服务")
            cat = None

        if cat:
            s = cat.get("stats", {})
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("实体", s.get("entities", 0))
            c2.metric("维度", s.get("dimensions", 0))
            c3.metric("指标", s.get("metrics", 0))
            c4.metric("业务域", s.get("businesses", 0))
            st.divider()

        sub_view, sub_add_m, sub_add_d = st.tabs(["📋 浏览 / 删除", "➕ 新增指标", "➕ 新增维度"])

        # ── 浏览 / 删除 ──────────────────────────────────────────────────────
        with sub_view:
            if not cat:
                st.info("暂无数据")
            else:
                view_m, view_d, view_biz = st.tabs([
                    f"📈 指标 ({len(cat.get('metrics',[]))})",
                    f"📐 维度 ({len(cat.get('dimensions',[]))})",
                    f"🏢 业务域 ({len(cat.get('business_domains',[]))})",
                ])

                with view_m:
                    for m in cat.get("metrics", []):
                        with st.expander(
                            f"**{m['label']}** `{m['name']}` "
                            f"— {m['metric_type']} / {m.get('complexity','normal')}",
                            expanded=False,
                        ):
                            col_info, col_del = st.columns([5, 1])
                            with col_info:
                                if m.get("expression"):
                                    st.code(m["expression"], language="sql")
                                st.write(
                                    f"**主表:** `{m.get('primary_table','?')}` AS `{m.get('primary_alias','t')}`  "
                                    f"**时间列:** `{m.get('time_column','')}`"
                                )
                                if m.get("compatible_dimensions"):
                                    st.write("**兼容维度:**", ", ".join(m["compatible_dimensions"]))
                                if m.get("synonyms"):
                                    st.write("**同义词:**", ", ".join(m["synonyms"]))
                                if m.get("extra_joins"):
                                    for j in m["extra_joins"]:
                                        st.code(
                                            f"{j['join_type']} {j['table']} {j['alias']} ON {j['on']}",
                                            language="sql",
                                        )
                            with col_del:
                                if st.button("🗑️", key=f"dm_{m['name']}", help="删除该指标"):
                                    try:
                                        r = _req.delete(
                                            f"{API_BASE}/semantic/node/metric/{m['name']}", timeout=10
                                        )
                                        r.raise_for_status()
                                        st.success(f"已删除 {m['name']}")
                                        st.rerun()
                                    except Exception as ex:
                                        st.error(str(ex))

                with view_d:
                    for d in cat.get("dimensions", []):
                        with st.expander(
                            f"**{d['label']}** `{d['name']}` — {d['dim_type']}"
                            + (f" / {d['grain']}" if d.get("grain") else ""),
                            expanded=False,
                        ):
                            col_info, col_del = st.columns([5, 1])
                            with col_info:
                                if d.get("expression"):
                                    st.code(d["expression"], language="sql")
                                if d.get("join"):
                                    j = d["join"]
                                    st.code(
                                        f"{j['join_type']} {j['table']} {j['alias']} ON {j['on']}",
                                        language="sql",
                                    )
                                if d.get("select_fields"):
                                    st.write("**取字段:**", ", ".join(d["select_fields"]))
                                if d.get("synonyms"):
                                    st.write("**同义词:**", ", ".join(d["synonyms"]))
                            with col_del:
                                if st.button("🗑️", key=f"dd_{d['name']}", help="删除该维度"):
                                    try:
                                        r = _req.delete(
                                            f"{API_BASE}/semantic/node/dimension/{d['name']}", timeout=10
                                        )
                                        r.raise_for_status()
                                        st.success(f"已删除 {d['name']}")
                                        st.rerun()
                                    except Exception as ex:
                                        st.error(str(ex))

                with view_biz:
                    for b in cat.get("business_domains", []):
                        with st.expander(f"**{b['label']}** `{b['name']}`"):
                            st.write("**关联指标:**", ", ".join(b.get("related_metrics", [])) or "—")
                            st.write("**关联维度:**", ", ".join(b.get("related_dimensions", [])) or "—")
                            if b.get("typical_questions"):
                                st.write("**典型问题关键词:**")
                                for q in b["typical_questions"]:
                                    st.caption(f"  · {q}")
                            if st.button("🗑️ 删除", key=f"db_{b['name']}"):
                                try:
                                    r = _req.delete(
                                        f"{API_BASE}/semantic/node/business/{b['name']}", timeout=10
                                    )
                                    r.raise_for_status()
                                    st.success(f"已删除 {b['name']}")
                                    st.rerun()
                                except Exception as ex:
                                    st.error(str(ex))

        # ── 新增指标 ──────────────────────────────────────────────────────────
        with sub_add_m:
            with st.form("sem_add_metric"):
                c1, c2 = st.columns(2)
                m_name   = c1.text_input("name（英文）*", placeholder="order_gmv")
                m_label  = c2.text_input("label（中文）*", placeholder="GMV")
                m_type   = c1.selectbox("metric_type", ["simple","ratio","derived","composite"])
                m_cmplx  = c2.selectbox("complexity", ["normal","high"])
                m_expr   = st.text_area("expression", placeholder="SUM({alias}.apportion_amt)")
                c3, c4   = st.columns(2)
                m_table  = c3.text_input("primary_table", placeholder="dwd_trade_order_wide")
                m_alias  = c4.text_input("primary_alias", value="o")
                m_tcol   = st.text_input("time_column", placeholder="o.dt 或 {alias}.dt")
                m_num    = st.text_input("numerator_expr（ratio 分子）")
                m_den    = st.text_input("denominator_expr（ratio 分母）")
                c5, c6   = st.columns(2)
                m_fmt    = c5.selectbox("output_format", ["number","currency","percent"])
                m_unit   = c6.text_input("unit", placeholder="元 / % / 件")
                m_compat = st.text_input("compatible_dimensions（逗号分隔）",
                                         placeholder="time_month,store_dim")
                m_syns   = st.text_input("synonyms（逗号分隔）")
                m_tags   = st.text_input("tags（逗号分隔）", placeholder="core,revenue")
                m_desc   = st.text_area("description（可选）", height=60)
                m_joins  = st.text_area(
                    "extra_joins JSON（可选）",
                    height=70,
                    placeholder='[{"table":"dim_store_info","alias":"s","join_type":"LEFT JOIN","on":"s.store_id = {fact_alias}.store_id"}]',
                )
                if st.form_submit_button("💾 保存到 DB", type="primary"):
                    if not m_name or not m_label:
                        st.error("name 和 label 为必填项")
                    else:
                        try:
                            extra_joins = json.loads(m_joins) if m_joins.strip() else []
                        except Exception:
                            st.error("extra_joins JSON 格式有误")
                            extra_joins = None
                        if extra_joins is not None:
                            payload = {
                                "name": m_name.strip(), "label": m_label.strip(),
                                "metric_type": m_type, "complexity": m_cmplx,
                                "expression": m_expr.strip(),
                                "primary_source": {"table": m_table.strip(), "alias": m_alias.strip()} if m_table.strip() else None,
                                "extra_joins": extra_joins,
                                "time_column": m_tcol.strip(),
                                "numerator_expr": m_num.strip(), "denominator_expr": m_den.strip(),
                                "output_format": m_fmt, "unit": m_unit.strip(),
                                "compatible_dimensions": [x.strip() for x in m_compat.split(",") if x.strip()],
                                "synonyms": [x.strip() for x in m_syns.split(",") if x.strip()],
                                "tags": [x.strip() for x in m_tags.split(",") if x.strip()],
                                "description": m_desc.strip(),
                            }
                            try:
                                r = _req.put(f"{API_BASE}/semantic/metric", json=payload, timeout=15)
                                r.raise_for_status()
                                st.success(f"✅ 指标 `{m_name}` 已写入 DB")
                                st.rerun()
                            except Exception as ex:
                                st.error(f"保存失败: {ex}")

        # ── 新增维度 ──────────────────────────────────────────────────────────
        with sub_add_d:
            with st.form("sem_add_dim"):
                c1, c2  = st.columns(2)
                d_name  = c1.text_input("name（英文）*", placeholder="store_dim")
                d_label = c2.text_input("label（中文）*", placeholder="门店维度")
                d_type  = c1.selectbox("dim_type", ["attribute","entity_ref","time"])
                d_grain = c2.selectbox("grain（time 类型用）", ["","day","month","quarter","year"])
                d_expr  = st.text_area(
                    "expression", height=60,
                    placeholder="time: DATE_FORMAT({time_col},'%Y-%m')\nattribute: {fact_alias}.category_id",
                )
                d_alias = st.text_input("alias（SELECT别名）", placeholder="stat_month")
                st.markdown("**JOIN 信息（entity_ref 类型填）**")
                c3, c4       = st.columns(2)
                d_join_table = c3.text_input("join.table", placeholder="dim_store_info")
                d_join_alias = c4.text_input("join.alias", placeholder="s")
                d_join_type  = c3.selectbox("join.join_type", ["LEFT JOIN","INNER JOIN","JOIN"])
                d_join_on    = c4.text_input("join.on", placeholder="{fact_alias}.store_id = s.store_id")
                d_sfields    = st.text_input("select_fields（逗号分隔）", placeholder="store_name,city")
                d_syns       = st.text_input("synonyms（逗号分隔）")
                d_tags       = st.text_input("tags（逗号分隔）", placeholder="store")
                d_desc       = st.text_area("description（可选）", height=60)
                if st.form_submit_button("💾 保存到 DB", type="primary"):
                    if not d_name or not d_label:
                        st.error("name 和 label 为必填项")
                    else:
                        join_obj = None
                        if d_join_table.strip():
                            join_obj = {
                                "table": d_join_table.strip(),
                                "alias": d_join_alias.strip(),
                                "join_type": d_join_type,
                                "on": d_join_on.strip(),
                            }
                        payload = {
                            "name": d_name.strip(), "label": d_label.strip(),
                            "dim_type": d_type, "grain": d_grain or None,
                            "expression": d_expr.strip(),
                            "alias": d_alias.strip() or d_name.strip(),
                            "join": join_obj,
                            "select_fields": [x.strip() for x in d_sfields.split(",") if x.strip()],
                            "synonyms": [x.strip() for x in d_syns.split(",") if x.strip()],
                            "tags": [x.strip() for x in d_tags.split(",") if x.strip()],
                            "description": d_desc.strip(),
                        }
                        try:
                            r = _req.put(f"{API_BASE}/semantic/dimension", json=payload, timeout=15)
                            r.raise_for_status()
                            st.success(f"✅ 维度 `{d_name}` 已写入 DB")
                            st.rerun()
                        except Exception as ex:
                            st.error(f"保存失败: {ex}")

    # ── 🤖 自动扫描 ────────────────────────────────────────────────────────────
    with tab_scan:
        st.subheader("🤖 从 Doris Schema 自动生成语义定义")
        st.caption(
            "读取 `information_schema`（表结构、字段注释）"
            " + `__internal_schema.audit_log`（历史 SQL 使用模式），"
            "自动生成指标/维度/实体草稿，预览后选择性写入 `semantic_store`。"
        )

        with st.expander("⚙️ 扫描参数", expanded=True):
            sc1, sc2, sc3 = st.columns(3)
            audit_limit  = sc1.number_input("分析 audit_log 条数", 100, 50000, 5000, 500)
            min_conf     = sc2.slider("最低置信度", 0.0, 1.0, 0.3, 0.05)
            incl_tbls    = sc3.text_input(
                "指定表（逗号分隔，留空=全部）",
                placeholder="dwd_trade_order_wide,dim_store_info",
            )

        if st.button("▶️ 开始扫描", type="primary", use_container_width=True):
            include_list = [t.strip() for t in incl_tbls.split(",") if t.strip()] or None
            with st.spinner("正在扫描 information_schema 和 audit_log …"):
                try:
                    r = _req.post(
                        f"{API_BASE}/semantic/scan",
                        json={"include_tables": include_list,
                              "audit_limit": int(audit_limit),
                              "min_confidence": float(min_conf),
                              "apply_to_db": False},
                        timeout=120,
                    )
                    r.raise_for_status()
                    st.session_state["sem_scan_result"] = r.json()
                except Exception as e:
                    st.error(f"扫描失败: {e}")

        scan_data = st.session_state.get("sem_scan_result")
        if scan_data:
            stats = scan_data.get("stats", {})
            st.success(
                f"扫描完成 | 表 {stats.get('tables_scanned',0)} 张 | "
                f"列 {stats.get('columns_scanned',0)} 个 | "
                f"草稿 {stats.get('proposals_total',0)} 条"
            )
            proposals = scan_data.get("proposals", [])
            if proposals:
                st.divider()
                by_type: dict = {"entity": [], "dimension": [], "metric": []}
                for p in proposals:
                    by_type.setdefault(p.get("node_type","metric"), []).append(p)

                tab_pe, tab_pd, tab_pm = st.tabs([
                    f"🏛️ 实体 ({len(by_type['entity'])})",
                    f"📐 维度 ({len(by_type['dimension'])})",
                    f"📈 指标 ({len(by_type['metric'])})",
                ])

                selected: list = []

                def _render_scan_tab(plist: list, pfx: str) -> list:
                    sel = []
                    for i, p in enumerate(plist):
                        conf = p.get("confidence", 0)
                        icon = "🟢" if conf >= 0.7 else "🟡" if conf >= 0.5 else "🔴"
                        chk = st.checkbox(
                            f"{icon} **{p['label']}** `{p['name']}` — {conf:.0%}",
                            value=(conf >= 0.6),
                            key=f"sc_{pfx}_{i}",
                        )
                        if chk:
                            sel.append(p)
                        with st.expander("详情", expanded=False):
                            st.caption(p.get("source", ""))
                            d = p.get("data", {})
                            if d.get("expression"):
                                st.code(d["expression"], language="sql")
                            if d.get("primary_source"):
                                ps = d["primary_source"]
                                st.write(f"主表: `{ps.get('table')}` AS `{ps.get('alias')}`")
                            if d.get("join"):
                                j = d["join"]
                                st.code(
                                    f"{j.get('join_type')} {j.get('table')} {j.get('alias')} ON {j.get('on')}",
                                    language="sql",
                                )
                    return sel

                with tab_pe:
                    selected += _render_scan_tab(by_type["entity"], "e")
                with tab_pd:
                    selected += _render_scan_tab(by_type["dimension"], "d")
                with tab_pm:
                    selected += _render_scan_tab(by_type["metric"], "m")

                st.divider()
                ca, cb, cc = st.columns(3)
                with ca:
                    if st.button(
                        f"✅ 写入 DB（已选 {len(selected)} 条）",
                        type="primary",
                        use_container_width=True,
                        disabled=len(selected) == 0,
                    ):
                        try:
                            r = _req.post(
                                f"{API_BASE}/semantic/scan/apply",
                                json={"proposals": selected},
                                timeout=30,
                            )
                            r.raise_for_status()
                            res = r.json()
                            st.success(
                                f"✅ 已写入 {res.get('applied',0)} 条 | "
                                f"catalog 统计: {res.get('catalog_stats',{})}"
                            )
                            st.session_state.pop("sem_scan_result", None)
                            st.rerun()
                        except Exception as ex:
                            st.error(f"写入失败: {ex}")
                with cb:
                    if st.button("📋 发送到同步管理/YAML编辑器", use_container_width=True):
                        import yaml as _yaml
                        entities  = [p["data"] for p in selected if p["node_type"]=="entity"]
                        dimensions= [p["data"] for p in selected if p["node_type"]=="dimension"]
                        metrics   = [p["data"] for p in selected if p["node_type"]=="metric"]
                        st.session_state["sem_yaml_content"] = _yaml.dump(
                            {"version":"1.0","db_name": scan_data.get("db_name",""),
                             "entities":entities,"dimensions":dimensions,
                             "metrics":metrics,"business":[]},
                            allow_unicode=True, default_flow_style=False, sort_keys=False,
                        )
                        st.success("✅ 已发送到「同步管理」→ YAML 编辑器，可在那里继续修改后写入 DB")
                with cc:
                    st.caption(f"共 {len(proposals)} 条草稿")

    # ── 📤 同步管理 ────────────────────────────────────────────────────────────
    with tab_sync:
        st.subheader("📤 同步管理")
        st.caption(
            "**DB → YAML**：把 `semantic_store` 当前状态导出为可编辑的 YAML。  \n"
            "**YAML → DB**：把编辑好的 YAML 写回 `semantic_store`，立即生效。"
        )

        # 操作按钮行
        btn1, btn2, btn3 = st.columns(3)

        with btn1:
            if st.button("⬇️ DB → 编辑器（导出）", use_container_width=True,
                         help="从 semantic_store 读取当前定义，加载到下方编辑器"):
                try:
                    r = _req.get(f"{API_BASE}/semantic/export", timeout=30)
                    r.raise_for_status()
                    st.session_state["sem_yaml_content"] = r.text
                    st.success("✅ 已导出，请在下方编辑器中查看 / 修改")
                except Exception as e:
                    st.error(f"导出失败: {e}")

        with btn2:
            if st.button("🔄 YAML 文件 → DB（重新加载）", use_container_width=True,
                         help="用本地 catalog_yaml/*.yaml 文件覆写 DB（首次初始化 / 回滚用）"):
                try:
                    r = _req.post(f"{API_BASE}/semantic/reload", timeout=30)
                    r.raise_for_status()
                    data = r.json()
                    st.success(f"✅ {data.get('message')} | {data.get('stats',{})}")
                except Exception as e:
                    st.error(f"重载失败: {e}")

        with btn3:
            # 查看当前统计
            if cat:
                s = cat.get("stats", {})
                st.metric("当前 DB 指标数", s.get("metrics", "—"))
            else:
                st.info("API 未连接")

        st.divider()

        # YAML 编辑器
        init_yaml = st.session_state.get(
            "sem_yaml_content",
            "# 点击「DB → 编辑器」加载当前语义定义，或直接粘贴 YAML 内容\n",
        )
        edited = st.text_area(
            "YAML 编辑器",
            value=init_yaml,
            height=550,
            key="sem_yaml_editor_area",
            help="修改后点「编辑器 → DB（导入）」写入 semantic_store",
        )

        ea, eb, ec = st.columns(3)

        with ea:
            if st.button("⬆️ 编辑器 → DB（导入）", type="primary",
                         use_container_width=True):
                if not edited.strip() or edited.startswith("# 点击"):
                    st.warning("请先加载或填入 YAML 内容")
                else:
                    try:
                        r = _req.post(
                            f"{API_BASE}/semantic/import",
                            json={"yaml_content": edited, "save_file": False},
                            timeout=30,
                        )
                        r.raise_for_status()
                        d = r.json()
                        st.session_state["sem_yaml_content"] = edited
                        st.success(f"✅ 已写入 DB | {d.get('stats',{})}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"导入失败: {e}")

        with eb:
            if st.button("💾 编辑器 → DB + 本地文件", use_container_width=True,
                         help="写入 DB 并同时覆写 catalog_yaml/*.yaml（版本备份）"):
                if not edited.strip() or edited.startswith("# 点击"):
                    st.warning("请先加载或填入 YAML 内容")
                else:
                    try:
                        r = _req.post(
                            f"{API_BASE}/semantic/import",
                            json={"yaml_content": edited, "save_file": True},
                            timeout=30,
                        )
                        r.raise_for_status()
                        d = r.json()
                        st.success(f"✅ {d.get('message')} | {d.get('stats',{})}")
                    except Exception as e:
                        st.error(f"保存失败: {e}")

        with ec:
            st.download_button(
                "⬇️ 下载 YAML 文件",
                data=edited,
                file_name="semantic_catalog.yaml",
                mime="text/yaml",
                use_container_width=True,
            )


# ════════════════════════════════════════════════════════════════════════════
# 🗄️ 元数据管理
# ════════════════════════════════════════════════════════════════════════════
elif page == "🗄️ 元数据管理":
    st.title("🗄️ 元数据管理")

    meta = get_metadata()
    tab_tables, tab_cols, tab_detail, tab_props = st.tabs([
        "表列表", "字段列表", "表详情", "Table Properties",
    ])

    with tab_tables:
        if st.button("🔄 重新同步"):
            meta._cache.clear()
        df = meta.to_dataframe()
        if not df.empty:
            st.caption(f"共 {len(df)} 张表")
            st.dataframe(df, use_container_width=True, height=500)
        else:
            st.info("暂无数据，请点击「重新同步」")

    with tab_cols:
        df_cols = meta.columns_dataframe()
        if not df_cols.empty:
            search = st.text_input("搜索字段名/表名/注释")
            if search:
                mask = df_cols.apply(
                    lambda row: search.lower() in str(row).lower(), axis=1
                )
                df_cols = df_cols[mask]
            st.caption(f"共 {len(df_cols)} 个字段")
            st.dataframe(df_cols, use_container_width=True, height=500)

    with tab_detail:
        tables = meta.all_tables()
        table_names = [m.table_name for m in tables]
        if table_names:
            selected = st.selectbox("选择表", table_names)
            if selected:
                m = meta.get_table(selected)
                if m:
                    st.markdown(f"### {m.table_name}")
                    st.caption(m.table_comment)
                    col1, col2, col3 = st.columns(3)
                    col1.metric("引擎", m.engine)
                    col2.metric("预估行数", f"{m.table_rows:,}")
                    col3.metric("字段数", len(m.columns))
                    st.markdown("**DDL 预览：**")
                    st.code(m.to_ddl(), language="sql")
                    st.markdown("**业务摘要：**")
                    st.info(m.summary())
        else:
            st.info("请先同步元数据")

    with tab_props:
        st.subheader("Doris 表属性（information_schema.table_properties）")
        if st.button("查询"):
            df_props = meta.get_table_properties()
            if not df_props.empty:
                st.dataframe(df_props, use_container_width=True)
            else:
                st.warning("无数据或该版本 Doris 不支持")


# ════════════════════════════════════════════════════════════════════════════
# 🔗 血缘分析
# ════════════════════════════════════════════════════════════════════════════
elif page == "🔗 血缘分析":
    st.title("🔗 表级血缘分析")

    lm = get_lineage()

    col_build, col_analysis = st.columns([2, 1])
    with col_build:
        col_btn1, col_btn2 = st.columns(2)
        with col_btn1:
            do_build = st.button("🏗️ 从 audit_log 构建血缘图")
        with col_btn2:
            do_diag = st.button("🔍 诊断 audit_log（0 条时点这里）")

        if do_build:
            with st.spinner("解析 SQL 血缘中..."):
                fetched, n = lm.build_from_audit_log_verbose(limit=3000)
                n2 = lm.build_from_vanna_knowledge()
            total = len(lm.graph.all_tables())
            # 展示诊断数字，无论成功与否
            st.info(
                f"audit_log 取回 SQL：**{fetched}** 条 ｜ 解析出 edge：**{n}** 条"
                f" ｜ Vanna 知识库：**{n2}** 条 ｜ 涉及表：**{total}** 张"
            )
            if fetched == 0:
                st.error("❌ 查询返回 0 行——连接或权限有问题，请查看后台日志。")
            elif n + n2 == 0:
                st.warning(
                    f"⚠️ 取回了 {fetched} 条 SQL，但解析出 0 条血缘。\n\n"
                    "原因：这些 SQL 均为 `INSERT INTO t VALUES(...)` 格式，没有 FROM 子句。\n"
                    "点击「🔍 诊断 audit_log」查看样本。"
                )
            else:
                st.success(f"✅ 血缘图构建完成，共 {total} 张表，{n + n2} 条依赖关系。")

        if do_diag:
            with st.spinner("分析 audit_log 中的 SQL 结构..."):
                diag = lm.diagnose_audit_log(limit=200)
            st.markdown("### 📊 audit_log 诊断报告")
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("取回 SQL 总数", diag["fetched"])
            c2.metric("含 FROM（可产生血缘）", diag["has_from"])
            c3.metric("纯 VALUES（不产生血缘）", diag["values_only"])
            c4.metric("实际解析 edge 数", diag["parsed_edges"])

            if diag["fetched"] == 0:
                st.error("❌ audit_log 里完全没有 INSERT/CREATE 语句，请确认 Doris 审计日志已开启。")
            elif diag["has_from"] == 0:
                st.error(
                    "❌ 取回的 SQL **全部是** `INSERT INTO t VALUES(...)` 格式，"
                    "无 FROM 子句，无法产生表级血缘。\n\n"
                    "血缘需要 ETL 型 SQL，例如：\n"
                    "```sql\n"
                    "INSERT INTO dw_table SELECT a, b FROM ods_table WHERE ...\n"
                    "```"
                )
            elif diag["parsed_edges"] == 0:
                st.warning(
                    "⚠️ 有含 FROM 的 SQL，但解析出的 edge 为 0。"
                    "请查看下方「含 FROM 的 SQL 样本」，可能是表名格式不匹配正则。"
                )
            else:
                st.success(f"✅ 应能解析出 {diag['parsed_edges']} 条血缘，请点击「构建血缘图」。")

            if diag["samples_with_from"]:
                with st.expander(f"✅ 含 FROM 的 SQL 样本（共 {diag['has_from']} 条，展示前5条）"):
                    for i, sql in enumerate(diag["samples_with_from"], 1):
                        st.code(sql, language="sql")
            if diag["samples_no_from"]:
                with st.expander(f"⚠️ 纯 VALUES 的 SQL 样本（共 {diag['values_only']} 条，展示前5条）"):
                    for i, sql in enumerate(diag["samples_no_from"], 1):
                        st.code(sql, language="sql")

    # 血缘图
    all_tables = sorted(lm.graph.all_tables())
    if all_tables:
        highlight = st.selectbox("高亮某张表（查看其上下游）",
                                 ["（不高亮）"] + all_tables)
        if highlight == "（不高亮）":
            highlight = ""

        fig = lm.to_plotly_figure(highlight_table=highlight)
        if fig:
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("无法生成图表")

        st.divider()
        col_t, col_i = st.columns(2)

        with col_t:
            st.subheader("📋 血缘关系明细")
            df_lin = lm.get_lineage_df()
            st.dataframe(df_lin, use_container_width=True, height=300)

        with col_i:
            st.subheader("🔍 影响分析")
            if all_tables:
                sel_table = st.selectbox("选择表", all_tables, key="impact_sel")
                impact = lm.impact_analysis(sel_table)
                st.metric("上游依赖", impact["upstream_count"])
                if impact["upstream_tables"]:
                    st.write("**上游表：**")
                    for t in impact["upstream_tables"]:
                        st.markdown(f"- `{t}`")
                st.metric("下游影响", impact["downstream_count"])
                if impact["downstream_tables"]:
                    st.write("**下游表：**")
                    for t in impact["downstream_tables"]:
                        st.markdown(f"- `{t}`")

        # 手动添加 SQL
        st.divider()
        st.subheader("➕ 手动添加 SQL 血缘")
        manual_sql = st.text_area("粘贴 SQL", height=100,
                                  placeholder="INSERT INTO dws_xxx SELECT ... FROM ods_xxx")
        if st.button("解析并添加"):
            if manual_sql:
                lm.add_sql(manual_sql)
                st.success("已添加到血缘图")
            else:
                st.warning("请输入 SQL")
    else:
        st.info("请先点击「从 audit_log 构建血缘图」")



# ════════════════════════════════════════════════════════════════════════════
elif page == "🔍 调试控制台":
    st.title("🔍 调试控制台")
    st.caption("输入自然语言问题，查看 Vanna Skill 完整调用链路")

    vn = get_vanna()

    question = st.text_input(
        "输入问题",
        placeholder="上个月各地区的销售总额分别是多少？",
        key="debug_q"
    )

    col_run, col_exec = st.columns(2)
    run_clicked = col_run.button("▶️ 生成 SQL（仅看链路）", type="primary",
                                 use_container_width=True)
    exec_clicked = col_exec.button("⚡ 生成 SQL 并执行", use_container_width=True)

    if (run_clicked or exec_clicked) and question:
        with st.spinner("正在推理..."):
            result = vn.ask_with_trace(question)

        sql = result.get("sql", "")
        trace = result.get("trace", {})
        error = result.get("error", "")

        # ── 调用链可视化 ──────────────────────────────────────────────────────
        st.divider()
        st.subheader("📡 完整调用链路")

        total_ms = trace.get("total_ms", 0)
        status = trace.get("status", "")
        status_icon = "✅" if status == "ok" else "❌"
        st.markdown(
            f"**Trace ID:** `{trace.get('trace_id', '')}` &nbsp; "
            f"**状态:** {status_icon} &nbsp; "
            f"**总耗时:** `{total_ms:.0f}ms`"
        )

        steps = trace.get("steps", [])
        if steps:
            st.markdown("---")
            for step in steps:
                step_icon = {
                    "ok": "✅", "error": "❌", "cached": "💾", "running": "⏳"
                }.get(step["status"], "⬜")
                note = f"  `{step['note']}`" if step.get("note") else ""
                dur = step.get("duration_ms", 0)

                with st.expander(
                    f"{step_icon} **{step['name']}** — {dur:.0f}ms{note}",
                    expanded=(step["status"] == "error")
                ):
                    if step.get("inputs"):
                        st.markdown("**📥 输入：**")
                        st.json(step["inputs"])
                    if step.get("outputs"):
                        st.markdown("**📤 输出：**")
                        st.json(step["outputs"])
                    if step.get("error"):
                        st.error(f"错误：{step['error']}")

        # ── 生成的 SQL ────────────────────────────────────────────────────────
        st.divider()
        st.subheader("📝 生成的 SQL")
        if sql:
            st.code(sql, language="sql")

            # 执行
            if exec_clicked:
                st.divider()
                st.subheader("📊 执行结果")
                with st.spinner("执行中..."):
                    try:
                        t0 = time.time()
                        df = vn.run_sql(sql)
                        exec_ms = (time.time() - t0) * 1000
                        st.caption(f"执行耗时 {exec_ms:.0f}ms，返回 {len(df)} 行")
                        st.dataframe(df, use_container_width=True)
                    except Exception as e:
                        st.error(f"执行失败: {e}")

            # 反馈
            st.divider()
            st.subheader("👍 结果反馈（加入知识库）")
            col_y, col_n = st.columns(2)
            if col_y.button("✅ SQL 正确，加入知识库"):
                try:
                    vn.add_question_sql(question, sql, source="feedback")
                    st.success("已加入 Vanna 知识库！下次类似问题会更准确")
                except Exception as e:
                    st.error(f"写入失败: {e}")
            if col_n.button("✏️ SQL 有误，我来修正"):
                corrected = st.text_area("修正后的 SQL", value=sql, height=100)
                if st.button("保存修正"):
                    try:
                        vn.add_question_sql(question, corrected, source="feedback")
                        st.success("修正版已保存！")
                    except Exception as e:
                        st.error(f"保存失败: {e}")
        elif error:
            st.error(f"生成失败：{error}")

    elif (run_clicked or exec_clicked) and not question:
        st.warning("请输入问题")


# ════════════════════════════════════════════════════════════════════════════
# 📋 调用日志
# ════════════════════════════════════════════════════════════════════════════
elif page == "📋 调用日志":
    st.title("📋 调用日志")

    col_r, col_n = st.columns([1, 3])
    with col_r:
        n = st.slider("显示最近 N 条", 10, 200, 50)
    with col_n:
        filter_status = st.multiselect(
            "状态筛选", ["ok", "error", "running"],
            default=["ok", "error"]
        )

    recent = tracer.recent(n)
    if filter_status:
        recent = [t for t in recent if t.status in filter_status]

    if not recent:
        st.info("暂无日志记录，请先在「调试控制台」发起请求")
    else:
        # 概览表格
        rows = []
        for t in recent:
            rows.append({
                "时间": t.created_at,
                "Trace ID": t.trace_id,
                "状态": {"ok": "✅", "error": "❌", "running": "⏳"}.get(t.status, "?"),
                "耗时(ms)": f"{t.total_ms:.0f}",
                "模型": t.model_used,
                "问题": t.question[:50],
                "SQL": (t.final_sql or "")[:60],
                "错误": t.error[:40] if t.error else "",
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, height=300)

        # 详情
        st.divider()
        st.subheader("单条详情")
        trace_ids = [t.trace_id for t in recent]
        sel_id = st.selectbox("选择 Trace ID", trace_ids)
        sel_trace = next((t for t in recent if t.trace_id == sel_id), None)

        if sel_trace:
            st.markdown(
                f"**Q:** {sel_trace.question}  \n"
                f"**SQL:** `{sel_trace.final_sql or '（无）'}`  \n"
                f"**错误:** {sel_trace.error or '无'}"
            )
            st.markdown("**步骤明细：**")
            for line in sel_trace.to_log_lines():
                st.text(line)
