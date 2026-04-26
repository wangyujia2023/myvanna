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
    "4月份各城市销售额是多少？",
    "今年4月份销售额是多少？",
    "昨日各城市销售额环比增长多少？",
    "PLUS会员和普通会员消费对比？",
    "帮我看看按门店类型划分的客单价",
    "4月份各城市销售额和净收入是多少？",
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
