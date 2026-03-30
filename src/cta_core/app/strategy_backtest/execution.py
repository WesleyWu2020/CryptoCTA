from __future__ import annotations

import argparse

from cta_core.app.strategy_presets import list_backtest_strategies
from cta_core.app.turtle_backtest import run_turtle_backtest, write_backtest_output
from cta_core.data.market_data_store import utc_ms

from .data_source import load_or_fetch


def resolve_execution_flags(args: argparse.Namespace) -> dict[str, bool | int | float | None]:
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


def execute_rp_daily_breakout(args: argparse.Namespace) -> int:
    if args.preset is not None and args.preset not in {preset.strategy_id for preset in list_backtest_strategies()}:
        raise ValueError(f"unknown preset '{args.preset}'")

    start_ms = utc_ms(args.start)
    end_ms = utc_ms(args.end)
    flags = resolve_execution_flags(args)

    bars, data_source = load_or_fetch(
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


__all__ = ["execute_rp_daily_breakout", "resolve_execution_flags"]
