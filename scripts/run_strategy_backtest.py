from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from cta_core.strategy_runtime.registry import build_strategy, list_strategy_ids


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Strategy runtime backtest shell")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--list-strategies", action="store_true", help="print registered strategy ids")
    group.add_argument("--strategy", choices=list_strategy_ids(), help="build a registered strategy")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.list_strategies:
        for strategy_id in list_strategy_ids():
            print(strategy_id)
        return 0

    strategy = build_strategy(args.strategy)
    print(strategy.strategy_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
