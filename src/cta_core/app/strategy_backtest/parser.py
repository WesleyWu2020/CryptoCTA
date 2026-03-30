from __future__ import annotations

import argparse
from pathlib import Path

from cta_core.app.strategy_presets import get_backtest_strategy, list_backtest_strategies
from cta_core.strategy_runtime.registry import list_strategy_ids


def matches_option(token: str, option: str) -> bool:
    return token == option or token.startswith(f"{option}=")


def _bootstrap_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--strategy", choices=list_strategy_ids())
    parser.add_argument(
        "--preset",
        choices=[preset.strategy_id for preset in list_backtest_strategies()],
    )
    parser.add_argument("--list-strategies", action="store_true")
    return parser.parse_known_args(argv)[0]


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    bootstrap = _bootstrap_args(argv)

    parser = argparse.ArgumentParser(description="Run registered strategy backtests.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--list-strategies", action="store_true", help="print registered strategy ids")
    group.add_argument("--strategy", choices=list_strategy_ids(), help="build or execute a registered strategy")
    parser.add_argument(
        "--preset",
        choices=[preset.strategy_id for preset in list_backtest_strategies()],
        help="apply preset defaults for supported strategy execution",
    )
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--interval", default="1d")
    parser.add_argument("--htf-interval", default="1d", help="not yet supported by the generic runner")
    parser.add_argument("--start", default="2024-09-01")
    parser.add_argument("--end", default="2026-03-16", help="exclusive date; use 2026-03-16 for up to 2026-03-15")
    parser.add_argument("--db-path", type=Path, default=Path("artifacts/market_data/klines.duckdb"))
    parser.add_argument("--use-binance", action="store_true", help="force fetch from Binance API instead of DuckDB")

    parser.add_argument("--initial-capital", type=float, default=100000.0)
    parser.add_argument("--risk-per-trade", type=float, default=0.005)
    parser.add_argument("--entry-lookback", type=int, default=55)
    parser.add_argument("--exit-lookback", type=int, default=20)
    parser.add_argument("--atr-lookback", type=int, default=20)
    parser.add_argument("--fee-bps", type=float, default=5.0)
    parser.add_argument("--slippage-bps", type=float, default=1.0)
    parser.add_argument("--stop-atr-multiple", type=float, default=2.0)
    parser.add_argument("--max-leverage", type=float, default=1.0)

    parser.add_argument("--allow-short", action="store_true")
    parser.add_argument("--disable-short", action="store_true")
    parser.add_argument("--trend-ema-window", type=int, default=200)
    parser.add_argument("--disable-trend-filter", action="store_true")
    parser.add_argument("--cooldown-bars", type=int, default=4)

    parser.add_argument("--pullback-window", type=int, default=12)
    parser.add_argument("--pullback-tolerance-atr", type=float, default=0.25)
    parser.add_argument("--breakout-entry-fraction", type=float, default=0.35)
    parser.add_argument("--expansion-bars", type=int, default=3)
    parser.add_argument("--expansion-min-growth", type=float, default=1.05)
    parser.add_argument("--disable-expansion-filter", action="store_true")
    parser.add_argument("--disable-trend-strength-layering", action="store_true")
    parser.add_argument("--strong-trend-threshold", type=float, default=6.0)
    parser.add_argument("--weak-exit-lookback", type=int)
    parser.add_argument("--strong-exit-lookback", type=int)
    parser.add_argument("--weak-pullback-tolerance-atr", type=float)
    parser.add_argument("--strong-pullback-tolerance-atr", type=float)
    parser.add_argument("--weak-trend-pullback-only", action="store_true")
    parser.add_argument("--weak-trend-no-trade", action="store_true")
    parser.add_argument("--add-on-only-after-profit", action="store_true")
    parser.add_argument("--addon-min-unrealized-r", type=float, default=0.5)
    parser.add_argument("--min-breakout-distance-atr", type=float, default=0.15)
    parser.add_argument("--min-breakout-body-atr", type=float, default=0.25)
    parser.add_argument("--enable-partial-take-profit", action="store_true")
    parser.add_argument("--take-profit-r-multiple", type=float, default=1.0)
    parser.add_argument("--take-profit-fraction", type=float, default=0.5)
    parser.add_argument("--disable-signal-score-filter", action="store_true")
    parser.add_argument("--min-signal-score-ratio", type=float, default=0.6)
    parser.add_argument("--min-position-scale", type=float, default=0.35)
    parser.add_argument("--follow-through-bars", type=int, default=2)
    parser.add_argument("--follow-through-max-wait-bars", type=int, default=3)
    parser.add_argument("--max-hold-bars", type=int, default=40)
    parser.add_argument("--rp-turnover-window", type=int, default=100)
    parser.add_argument("--rp-base-turnover", type=float, default=0.02)
    parser.add_argument("--rp-max-turnover-cap", type=float, default=0.8)
    parser.add_argument("--rp-entry-confirm-bars", type=int, default=3)
    parser.add_argument("--rp-exit-confirm-bars", type=int, default=3)
    parser.add_argument("--rp-entry-band-atr", type=float, default=0.0)
    parser.add_argument("--rp-exit-band-atr", type=float, default=0.0)
    parser.add_argument("--rp-min-hold-bars", type=int, default=0)
    parser.add_argument("--rp-htf-slope-bars", type=int, default=1)
    parser.add_argument("--enable-rp-chop-filter", action="store_true")
    parser.add_argument("--disable-rp-chop-filter", action="store_true")
    parser.add_argument("--rp-slope-bars", type=int, default=3)
    parser.add_argument("--rp-min-slope-ratio", type=float, default=0.0005)
    parser.add_argument("--rp-min-atr-ratio", type=float, default=0.008)
    parser.add_argument("--enable-rp-signal-quality-sizing", action="store_true")
    parser.add_argument("--disable-rp-signal-quality-sizing", action="store_true")
    parser.add_argument("--rp-quality-target-atr", type=float, default=1.0)
    parser.add_argument("--rp-quality-min-scale", type=float, default=0.35)
    parser.add_argument("--disable-regime-filter", action="store_true")
    parser.add_argument("--regime-ema-window", type=int, default=30)
    parser.add_argument("--regime-slope-bars", type=int, default=3)
    parser.add_argument("--regime-min-slope", type=float, default=0.002)
    parser.add_argument("--enable-vol-target-sizing", action="store_true")
    parser.add_argument("--disable-vol-target-sizing", action="store_true")
    parser.add_argument("--target-annual-vol", type=float, default=0.15)
    parser.add_argument("--vol-target-window", type=int, default=20)
    parser.add_argument("--min-position-allocation", type=float, default=0.2)

    parser.add_argument("--disable-htf-filter", action="store_true", help="not yet supported by the generic runner")
    parser.add_argument("--htf-entry-lookback", type=int, default=20, help="not yet supported by the generic runner")
    parser.add_argument("--htf-expansion-bars", type=int, default=3, help="not yet supported by the generic runner")
    parser.add_argument(
        "--htf-expansion-min-growth",
        type=float,
        default=1.05,
        help="not yet supported by the generic runner",
    )
    parser.add_argument(
        "--disable-htf-expansion-filter",
        action="store_true",
        help="not yet supported by the generic runner",
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=Path("artifacts/backtests/btcusdt_1d_rp_break_2024-09-01_2026-03-15_fee5.json"),
    )

    if bootstrap.preset is not None:
        parser_dests = {action.dest for action in parser._actions}
        preset_defaults = {
            key: value
            for key, value in get_backtest_strategy(bootstrap.preset).defaults.items()
            if key in parser_dests
        }
        parser.set_defaults(**preset_defaults)

    return parser.parse_args(argv)


__all__ = ["matches_option", "parse_args"]
