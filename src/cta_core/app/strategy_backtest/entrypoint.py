from __future__ import annotations

import sys

from cta_core.strategy_runtime.registry import build_strategy, get_strategy_class, list_strategy_ids

from .constants import SUPPORTED_EXECUTION_STRATEGIES, UNSUPPORTED_HTF_EXECUTION_OPTIONS
from .execution import (
    execute_liquidation_vacuum_reversion,
    execute_liquidity_shock_reversion,
    execute_rp_daily_breakout,
    execute_rsi_threshold,
    execute_smart_money_size_breakout,
    execute_taker_imbalance_absorption,
)
from .parser import parse_args


def _matches_option(token: str, option: str) -> bool:
    return token == option or token.startswith(f"{option}=")


def _should_execute(argv: list[str] | None) -> bool:
    if argv is None:
        argv = sys.argv[1:]

    index = 0
    while index < len(argv):
        token = argv[index]
        if _matches_option(token, "--strategy"):
            index += 1 if token.startswith("--strategy=") else 2
            continue
        if token == "--list-strategies":
            index += 1
            continue
        return True
    return False


def _collect_unsupported_execution_options(argv: list[str] | None) -> list[str]:
    if argv is None:
        argv = sys.argv[1:]

    used_options: list[str] = []
    for token in argv:
        for option in UNSUPPORTED_HTF_EXECUTION_OPTIONS:
            if _matches_option(token, option) and option not in used_options:
                used_options.append(option)
    return used_options


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.list_strategies:
        for strategy_id in list_strategy_ids():
            print(strategy_id)
        return 0

    if not _should_execute(argv):
        strategy = build_strategy(args.strategy)
        print(strategy.strategy_id)
        return 0

    if args.strategy not in SUPPORTED_EXECUTION_STRATEGIES:
        supported = ", ".join(sorted(SUPPORTED_EXECUTION_STRATEGIES))
        print(
            f"strategy execution is not yet supported for '{args.strategy}'; supported: {supported}",
            file=sys.stderr,
        )
        return 2

    unsupported_options = _collect_unsupported_execution_options(argv)
    if unsupported_options:
        print(
            "HTF execution options are not yet supported by the generic runner: "
            + ", ".join(unsupported_options),
            file=sys.stderr,
        )
        return 2

    if args.strategy == "rp_daily_breakout":
        return execute_rp_daily_breakout(args)
    if args.strategy == "rsi_threshold":
        return execute_rsi_threshold(args)
    if args.strategy == "liquidity_shock_reversion":
        return execute_liquidity_shock_reversion(args)
    if args.strategy == "taker_imbalance_absorption":
        return execute_taker_imbalance_absorption(args)
    if args.strategy == "liquidation_vacuum_reversion":
        return execute_liquidation_vacuum_reversion(args)
    if args.strategy == "smart_money_size_breakout":
        return execute_smart_money_size_breakout(args)
    print(f"strategy execution routing missing for '{args.strategy}'", file=sys.stderr)
    return 2


__all__ = ["main"]
