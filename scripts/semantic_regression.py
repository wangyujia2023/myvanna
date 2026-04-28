"""
语义层回归脚本

运行:
  ./venv/bin/python scripts/semantic_regression.py
  ./venv/bin/python scripts/semantic_regression.py --question "4月份各城市销售额是多少？"
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from vanna_skill import load_config
from vanna_skill.pipelines.semantic_pipeline import SemanticPipeline


DEFAULT_CASES = [
    "每个城市销售额排名前三的门店分别是哪些？",
    "4月份各城市销售额是多少",
    "昨日各城市销售额各环比同期增长多少？",
    "PLUS会员和普通会员消费对比？",
    "今天的总交易额是多少？",
    "按一级类目查看商品销量。",
    "不同门店类型的销售额分布是怎样的？",
    "北京，PLUS会员的总消费金额是多少？",
    "帮我看看北京的总消费金额是多少？其中 PLUS 会员消费了多少？",
    "上个月的笔单价和成交笔数分别多少？",
    "卖得最好的前5个城市是哪些？",
    "查看各城市、各门店类型下的总收入和客单价。",
    "今年每个月的GMV趋势是怎样的？",
    "普通会员购买一级类目为101的商品，花了多少钱？",
    "昨日各城市销售额环比同期增长多少？",
    "上海会员、非会员的总消费金额分别是多少，对比值？",
    "高价值用户流失风险与消费异动深度归因分析",
    "上个月各地区销售总额排名第一的门店？"
]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--question", action="append", help="指定要回归的问句，可多次传入")
    args = parser.parse_args()

    questions = args.question or DEFAULT_CASES
    pipeline = SemanticPipeline(load_config())

    print("=" * 80)
    print("Semantic Regression")
    print("=" * 80)

    failed = 0
    for idx, question in enumerate(questions, start=1):
        result = pipeline.run(question)
        trace = result.get("trace", {})
        steps = trace.get("steps", [])
        semantic_step = next((s for s in steps if s.get("name") == "semantic_parse"), {})
        print(f"\n[{idx}] {question}")
        print(f"  path   : {result.get('path')}")
        print(f"  error  : {result.get('error') or '-'}")
        print(f"  sql    :\n{result.get('sql') or '-'}")
        if semantic_step:
            print(f"  parse  : {semantic_step.get('outputs')}")
        if result.get("error"):
            failed += 1

    print("\n" + "-" * 80)
    print(f"Total: {len(questions)} | Failed: {failed} | Passed: {len(questions) - failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
