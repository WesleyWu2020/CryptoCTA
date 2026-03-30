from __future__ import annotations

import argparse
import sys

from run_strategy_backtest import main as run_strategy_backtest_main

ROOT_STRATEGY_ID = "rp_daily_breakout"


def _bootstrap_args(argv: list[str] | None = None) -> tuple[str, bool]:
    from cta_core.app.strategy_presets import list_backtest_strategies

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "--strategy",
        choices=[preset.strategy_id for preset in list_backtest_strategies()],
        default="rp_live",
    )
    parser.add_argument("--list-strategies", action="store_true")
    args, _ = parser.parse_known_args(argv)
    return str(args.strategy), bool(args.list_strategies)


def _print_strategies() -> None:
    from cta_core.app.strategy_presets import list_backtest_strategies

    for preset in list_backtest_strategies():
        print(f"{preset.strategy_id}: {preset.description}")


def _matches_option(token: str, option: str) -> bool:
    return token == option or token.startswith(f"{option}=")


def _forward_argv(argv: list[str] | None = None) -> list[str]:
    if argv is None:
        argv = sys.argv[1:]

    strategy_id, _ = _bootstrap_args(argv)

    from cta_core.app.strategy_presets import get_backtest_strategy

    preset = get_backtest_strategy(strategy_id)
    forwarded = ["--strategy", ROOT_STRATEGY_ID, "--preset", preset.strategy_id]

    skip_next = False
    for token in argv:
        if skip_next:
            skip_next = False
            continue
        if _matches_option(token, "--strategy"):
            if token == "--strategy":
                skip_next = True
            continue
        forwarded.append(token)

    return forwarded


def main(argv: list[str] | None = None) -> int:
    _, list_only = _bootstrap_args(argv)
    if list_only:
        _print_strategies()
        return 0
    return run_strategy_backtest_main(_forward_argv(argv))


if __name__ == "__main__":
    raise SystemExit(main())
