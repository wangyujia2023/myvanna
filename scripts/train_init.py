"""
初始化训练脚本 - 仅需运行一次
1. 从 information_schema 同步所有表的 DDL 和摘要
2. 从 audit_log 挖掘历史 SQL（可选）
3. 加载人工维护的 Q&A 对（可选）

运行: python scripts/train_init.py
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s  %(message)s")

from vanna_skill import DorisVanna, MetadataManager, AuditMiner, load_config

CONFIG = load_config()

# ── 预置的人工 Q&A 对（业务专家维护）────────────────────────────────────────
MANUAL_QA = [
    ("上个月各大区的销售总额分别是多少？",
     """SELECT si.region_id, SUM(t.pay_amt) AS total_amt
        FROM dws_store_traffic_trade_daily t
        JOIN dim_store_info si ON t.store_id = si.store_id
        WHERE DATE_FORMAT(t.dt, '%Y-%m') = DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 1 MONTH), '%Y-%m')
        GROUP BY si.region_id
        ORDER BY total_amt DESC"""),

    ("近7天哪些商品的销量最高？",
     """SELECT s.sku_name, SUM(d.sale_qty) AS total_qty
        FROM dws_sku_trade_daily d
        JOIN dim_sku_info s ON d.sku_id = s.sku_id
        WHERE d.dt >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
        GROUP BY s.sku_name
        ORDER BY total_qty DESC
        LIMIT 10"""),

    ("今天的GMV、订单量、下单人数是多少？",
     """SELECT total_gmv, total_orders, total_buyers
        FROM ads_sales_cockpit
        WHERE dt = CURDATE()"""),

    ("昨天各支付渠道的支付金额占比？",
     """SELECT pay_method, COUNT(*) AS cnt,
               SUM(1) / SUM(SUM(1)) OVER () AS pct
        FROM ods_payment_log
        WHERE dt = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
        GROUP BY pay_method
        ORDER BY cnt DESC"""),

    ("PLUS 会员和普通会员的消费金额对比？",
     """SELECT u.is_plus_vip,
               COUNT(DISTINCT t.user_id) AS user_cnt,
               SUM(t.pay_amount) AS total_pay,
               AVG(t.pay_amount) AS avg_pay
        FROM dws_user_trade_daily t
        JOIN dim_user_info u ON t.user_id = u.user_id
        WHERE t.dt >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
        GROUP BY u.is_plus_vip"""),

    ("上个月退款率最高的商品类目是哪些？",
     """SELECT s.category_1_id,
               COUNT(r.refund_id) AS refund_cnt,
               SUM(r.refund_amt) AS refund_amt
        FROM dwd_trade_refund_fact r
        JOIN dwd_trade_order_wide w ON r.order_id = w.order_id
        JOIN dim_sku_info s ON w.sku_id = s.sku_id
        WHERE r.dt >= DATE_FORMAT(DATE_SUB(CURDATE(), INTERVAL 1 MONTH), '%Y-%m-01')
        GROUP BY s.category_1_id
        ORDER BY refund_cnt DESC
        LIMIT 10"""),

    ("哪些用户最近30天没有下单（流失用户）？",
     """SELECT u.user_id, u.city_code, u.is_plus_vip
        FROM dim_user_info u
        WHERE u.user_id NOT IN (
            SELECT DISTINCT user_id FROM dws_user_trade_daily
            WHERE dt >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
        )
        LIMIT 1000"""),

    ("用户从首页到支付的整体转化率？",
     """SELECT platform, home_uv, pay_uv,
               ROUND(pay_uv / home_uv * 100, 2) AS conversion_rate
        FROM ads_user_funnel
        WHERE dt = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
        ORDER BY conversion_rate DESC"""),
]

_DDL_MAX_LEN = 3000  # 单个 DDL 超过此长度则截断（避免超 Embedding token 限制）


def _truncate(text: str, max_len: int = _DDL_MAX_LEN) -> str:
    if len(text) <= max_len:
        return text
    return text[:max_len] + "\n-- [DDL truncated]"


def test_llm(vn: DorisVanna) -> bool:
    """预检 Qwen Embedding 是否可用"""
    print("\n[预检] 测试 Qwen Embedding 连通性...", end=" ", flush=True)
    try:
        vec = vn._gemini.get_embedding("hello")
        print(f"✓ OK，向量维度 = {len(vec)}")
        return True
    except Exception as e:
        print(f"✗ 失败: {e}")
        return False


def main():
    print("=" * 60)
    print("Vanna Skill 初始化训练")
    print("=" * 60)

    vn = DorisVanna(CONFIG)

    # ── 预检 Gemini ──────────────────────────────────────────────────────────
    if not test_llm(vn):
        print("\n❌ Qwen 不可用，请检查 API Key 或网络，退出。")
        sys.exit(1)

    # ── Step 1: 同步 information_schema ────────────────────────────────────
    print("\n[1/3] 同步 information_schema 元数据...")
    meta = MetadataManager(vn._biz, CONFIG["database"])
    tables = meta.sync()
    print(f"  发现 {len(tables)} 张表，开始写入 Embedding...\n")

    ok_count, fail_count = 0, 0
    for i, m in enumerate(tables, 1):
        ddl = _truncate(m.to_ddl())
        summary = m.summary()
        prefix = f"  [{i:2d}/{len(tables)}] {m.table_name}"
        print(f"{prefix}  DDL写入...", end=" ", flush=True)
        try:
            vn.add_ddl(ddl, source="schema")
            print("✓DDL", end="  ", flush=True)
        except Exception as e:
            print(f"✗DDL({e})", end="  ", flush=True)
            fail_count += 1

        print("摘要写入...", end=" ", flush=True)
        try:
            vn.add_documentation(summary, source="schema")
            print("✓摘要")
            ok_count += 1
        except Exception as e:
            print(f"✗摘要({e})")
            fail_count += 1

    print(f"\n  完成：成功 {ok_count}，失败 {fail_count}")

    # ── Step 2: 加载人工 Q&A 对 ──────────────────────────────────────────────
    print(f"\n[2/3] 加载 {len(MANUAL_QA)} 条人工 Q&A 对...")
    for i, (q, sql) in enumerate(MANUAL_QA, 1):
        print(f"  [{i:2d}] {q[:40]}... ", end="", flush=True)
        try:
            vn.add_question_sql(q, sql, source="manual")
            print("✓")
        except Exception as e:
            print(f"✗ {e}")

    # ── Step 3: 挖掘 audit_log（可选）───────────────────────────────────────
    answer = input("\n[3/3] 是否挖掘 audit_log？(y/N): ").strip().lower()
    if answer == "y":
        miner = AuditMiner(vn._biz, vn)
        result = miner.mine(limit=500, auto_generate_question=True)
        print(f"  挖掘结果: {result}")
    else:
        print("  跳过 audit_log 挖掘")

    print("\n" + "=" * 60)
    print("✅ 初始化完成！")
    stats = vn.gemini_stats
    print(f"   Embedding 调用: {stats['embed_calls']} 次")
    print(f"   缓存命中率: {stats['embed_cache_hit_rate']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
