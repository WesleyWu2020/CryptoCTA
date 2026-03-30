from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from cta_core.app.strategy_presets import get_backtest_strategy, list_backtest_strategies
from cta_core.app.turtle_backtest import run_turtle_backtest, write_backtest_output
from cta_core.data.binance_client import BinanceUMClient
from cta_core.data.market_data_store import fetch_klines_range, utc_ms
from cta_core.strategy_runtime.registry import build_strategy, list_strategy_ids

_SUPPORTED_EXECUTION_STRATEGIES = {"rp_daily_breakout"}


def _load_bars_from_duckdb(
    *,
    db_path: Path,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
) -> pl.DataFrame:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        table = conn.execute(
            """
            SELECT open_time, open, high, low, close, volume
            FROM futures_klines
            WHERE symbol = ?
              AND interval = ?
              AND open_time >= ?
              AND open_time < ?
            ORDER BY open_time
            """,
            [symbol, interval, start_ms, end_ms],
        ).to_arrow_table()
    finally:
        conn.close()
    return pl.from_arrow(table)


def _load_or_fetch(
    *,
    db_path: Path,
    use_binance: bool,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
) -> tuple[pl.DataFrame, str]:
    if (not use_binance) and db_path.exists():
        return (
            _load_bars_from_duckdb(
                db_path=db_path,
                symbol=symbol,
                interval=interval,
                start_ms=start_ms,
                end_ms=end_ms,
            ),
            f"duckdb:{db_path}",
        )

    client = BinanceUMClient()
    bars = fetch_klines_range(
        client=client,
        symbol=symbol,
        interval=interval,
        start_ms=start_ms,
        end_ms=end_ms,
        limit=1500,
    ).select("open_time", "open", "high", "low", "close", "volume")
    return bars, "binance_api"


def _bootstrap_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--strategy", choices=list_strategy_ids())
    parser.add_argument(
        "--preset",
        choices=[preset.strategy_id for preset in list_backtest_strategies()],
    )
    parser.add_argument("--list-strategies", action="store_true")
    return parser.parse_known_args(argv)[0]


def _matches_option(token: str, option: str) -> bool:
    return token == option or token.startswith(f"{option}=")


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
    parser.add_argument("--htf-interval", default="1d")
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

    parser.add_argument("--disable-htf-filter", action="store_true")
    parser.add_argument("--htf-entry-lookback", type=int, default=20)
    parser.add_argument("--htf-expansion-bars", type=int, default=3)
    parser.add_argument("--htf-expansion-min-growth", type=float, default=1.05)
    parser.add_argument("--disable-htf-expansion-filter", action="store_true")

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


def _resolve_execution_flags(args: argparse.Namespace) -> dict[str, bool | int | float | None]:
    allow_short = False
    if args.allow_short:
        allow_short = True
    elif args.disable_short:
        allow_short = False

    trend_ema_window = None if args.disable_trend_filter else args.trend_ema_window
    require_channel_expansion = not args.disable_expansion_filter
    htf_require_channel_expansion = not args.disable_htf_expansion_filter
    use_trend_strength_layering = not args.disable_trend_strength_layering
    use_signal_score_filter = not args.disable_signal_score_filter

    use_rp_chop_filter = False
    if args.enable_rp_chop_filter:
        use_rp_chop_filter = True
    elif args.disable_rp_chop_filter:
        use_rp_chop_filter = False

    use_rp_signal_quality_sizing = False
    if args.enable_rp_signal_quality_sizing:
        use_rp_signal_quality_sizing = True
    elif args.disable_rp_signal_quality_sizing:
        use_rp_signal_quality_sizing = False

    use_regime_filter = not args.disable_regime_filter

    use_vol_target_sizing = False
    if args.enable_vol_target_sizing:
        use_vol_target_sizing = True
    elif args.disable_vol_target_sizing:
        use_vol_target_sizing = False

    return {
        "allow_short": allow_short,
        "trend_ema_window": trend_ema_window,
        "require_channel_expansion": require_channel_expansion,
        "htf_require_channel_expansion": htf_require_channel_expansion,
        "use_trend_strength_layering": use_trend_strength_layering,
        "use_signal_score_filter": use_signal_score_filter,
        "use_rp_chop_filter": use_rp_chop_filter,
        "use_rp_signal_quality_sizing": use_rp_signal_quality_sizing,
        "use_regime_filter": use_regime_filter,
        "use_vol_target_sizing": use_vol_target_sizing,
    }


def _execute_rp_daily_breakout(args: argparse.Namespace) -> int:
    if args.preset is not None and args.preset not in {preset.strategy_id for preset in list_backtest_strategies()}:
        raise ValueError(f"unknown preset '{args.preset}'")

    start_ms = utc_ms(args.start)
    end_ms = utc_ms(args.end)
    flags = _resolve_execution_flags(args)

    bars, data_source = _load_or_fetch(
        db_path=args.db_path,
        use_binance=args.use_binance,
        symbol=args.symbol,
        interval=args.interval,
        start_ms=start_ms,
        end_ms=end_ms,
    )

    result = run_turtle_backtest(
        bars=bars,
        bars_htf=None,
        symbol=args.symbol,
        interval=args.interval,
        entry_lookback=args.entry_lookback,
        exit_lookback=args.exit_lookback,
        atr_lookback=args.atr_lookback,
        initial_capital=args.initial_capital,
        risk_per_trade=args.risk_per_trade,
        fee_bps=args.fee_bps,
        slippage_bps=args.slippage_bps,
        stop_atr_multiple=args.stop_atr_multiple,
        max_leverage=args.max_leverage,
        allow_short=bool(flags["allow_short"]),
        trend_ema_window=flags["trend_ema_window"],
        cooldown_bars=args.cooldown_bars,
        pullback_window=args.pullback_window,
        pullback_tolerance_atr=args.pullback_tolerance_atr,
        breakout_entry_fraction=args.breakout_entry_fraction,
        require_channel_expansion=bool(flags["require_channel_expansion"]),
        expansion_bars=args.expansion_bars,
        expansion_min_growth=args.expansion_min_growth,
        use_trend_strength_layering=bool(flags["use_trend_strength_layering"]),
        strong_trend_threshold=args.strong_trend_threshold,
        weak_exit_lookback=args.weak_exit_lookback,
        strong_exit_lookback=args.strong_exit_lookback,
        weak_pullback_tolerance_atr=args.weak_pullback_tolerance_atr,
        strong_pullback_tolerance_atr=args.strong_pullback_tolerance_atr,
        weak_trend_pullback_only=args.weak_trend_pullback_only,
        weak_trend_no_trade=args.weak_trend_no_trade,
        add_on_only_after_profit=args.add_on_only_after_profit,
        addon_min_unrealized_r=args.addon_min_unrealized_r,
        min_breakout_distance_atr=args.min_breakout_distance_atr,
        min_breakout_body_atr=args.min_breakout_body_atr,
        enable_partial_take_profit=args.enable_partial_take_profit,
        take_profit_r_multiple=args.take_profit_r_multiple,
        take_profit_fraction=args.take_profit_fraction,
        use_signal_score_filter=bool(flags["use_signal_score_filter"]),
        min_signal_score_ratio=args.min_signal_score_ratio,
        min_position_scale=args.min_position_scale,
        follow_through_bars=args.follow_through_bars,
        follow_through_max_wait_bars=args.follow_through_max_wait_bars,
        max_hold_bars=args.max_hold_bars,
        use_htf_filter=False,
        htf_entry_lookback=args.htf_entry_lookback,
        htf_expansion_bars=args.htf_expansion_bars,
        htf_expansion_min_growth=args.htf_expansion_min_growth,
        htf_require_channel_expansion=bool(flags["htf_require_channel_expansion"]),
        rp_turnover_window=args.rp_turnover_window,
        rp_base_turnover=args.rp_base_turnover,
        rp_max_turnover_cap=args.rp_max_turnover_cap,
        rp_entry_confirm_bars=args.rp_entry_confirm_bars,
        rp_exit_confirm_bars=args.rp_exit_confirm_bars,
        rp_entry_band_atr=args.rp_entry_band_atr,
        rp_exit_band_atr=args.rp_exit_band_atr,
        rp_min_hold_bars=args.rp_min_hold_bars,
        rp_htf_slope_bars=args.rp_htf_slope_bars,
        use_rp_chop_filter=bool(flags["use_rp_chop_filter"]),
        rp_slope_bars=args.rp_slope_bars,
        rp_min_slope_ratio=args.rp_min_slope_ratio,
        rp_min_atr_ratio=args.rp_min_atr_ratio,
        use_rp_signal_quality_sizing=bool(flags["use_rp_signal_quality_sizing"]),
        rp_quality_target_atr=args.rp_quality_target_atr,
        rp_quality_min_scale=args.rp_quality_min_scale,
        use_regime_filter=bool(flags["use_regime_filter"]),
        regime_ema_window=args.regime_ema_window,
        regime_slope_bars=args.regime_slope_bars,
        regime_min_slope=args.regime_min_slope,
        use_vol_target_sizing=bool(flags["use_vol_target_sizing"]),
        target_annual_vol=args.target_annual_vol,
        vol_target_window=args.vol_target_window,
        min_position_allocation=args.min_position_allocation,
    )

    result["data_source"] = data_source
    result["data_source_htf"] = None

    path = write_backtest_output(result, args.output)
    print(path)
    print(result["summary"])
    return 0


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

    if args.strategy not in _SUPPORTED_EXECUTION_STRATEGIES:
        print(
            f"strategy execution is not yet supported for '{args.strategy}'; supported: rp_daily_breakout",
            file=sys.stderr,
        )
        return 2

    return _execute_rp_daily_breakout(args)


if __name__ == "__main__":
    raise SystemExit(main())
