from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, ClassVar

import polars as pl

from cta_core.strategy_runtime.base import BaseStrategy, StrategyContext, StrategyDecision, StrategyDecisionType


@dataclass(frozen=True)
class RPDailyBreakoutConfig:
    rp_window: int = 3
    entry_confirmations: int = 2
    exit_confirmations: int = 2
    quantity: Decimal = Decimal("1")

    def __post_init__(self) -> None:
        if self.rp_window <= 0:
            raise ValueError("rp_window must be > 0")
        if self.entry_confirmations <= 0:
            raise ValueError("entry_confirmations must be > 0")
        if self.exit_confirmations <= 0:
            raise ValueError("exit_confirmations must be > 0")
        if self.quantity <= 0:
            raise ValueError("quantity must be > 0")


@dataclass
class RPDailyBreakoutStrategy(BaseStrategy):
    config: RPDailyBreakoutConfig = field(default_factory=RPDailyBreakoutConfig)
    strategy_id: ClassVar[str] = "rp_daily_breakout"
    _long_open_by_symbol: dict[str, bool] = field(init=False, default_factory=dict, repr=False, compare=False)

    def prepare_features(self, bars: pl.DataFrame, bars_htf: pl.DataFrame | None = None) -> pl.DataFrame:
        if "close" not in bars.columns:
            raise ValueError("bars must contain close")

        closes = bars.get_column("close").to_list()
        rp_values: list[float] = []
        close_above_rp: list[bool] = []
        close_below_rp: list[bool] = []
        above_rp_streak: list[int] = []
        below_rp_streak: list[int] = []
        above_rp_confirmed: list[bool] = []
        below_rp_confirmed: list[bool] = []

        above_run = 0
        below_run = 0
        for index, close_value in enumerate(closes):
            close = float(close_value)
            if index == 0:
                rp = close
            else:
                start = max(0, index - self.config.rp_window)
                window = [float(value) for value in closes[start:index]]
                rp = sum(window) / len(window)

            rp_values.append(rp)

            is_above = close > rp
            is_below = close < rp
            close_above_rp.append(is_above)
            close_below_rp.append(is_below)

            if is_above:
                above_run += 1
                below_run = 0
            elif is_below:
                below_run += 1
                above_run = 0
            else:
                above_run = 0
                below_run = 0

            above_rp_streak.append(above_run)
            below_rp_streak.append(below_run)
            above_rp_confirmed.append(above_run >= self.config.entry_confirmations)
            below_rp_confirmed.append(below_run >= self.config.exit_confirmations)

        return bars.with_columns(
            pl.Series("rp", rp_values),
            pl.Series("close_above_rp", close_above_rp),
            pl.Series("close_below_rp", close_below_rp),
            pl.Series("above_rp_streak", above_rp_streak),
            pl.Series("below_rp_streak", below_rp_streak),
            pl.Series("above_rp_confirmed", above_rp_confirmed),
            pl.Series("below_rp_confirmed", below_rp_confirmed),
        )

    def on_start(self, context: StrategyContext) -> None:
        self.set_long_open(symbol=context.symbol, is_open=False)

    def on_bar(self, context: StrategyContext) -> list[StrategyDecision]:
        if context.bars.height == 0:
            return []

        current_bar = context.current_bar
        long_open = self._long_open_by_symbol.get(context.symbol, False)

        if bool(current_bar.get("above_rp_confirmed")) and not long_open:
            self._long_open_by_symbol[context.symbol] = True
            return [
                StrategyDecision(
                    decision_type=StrategyDecisionType.ENTER_LONG,
                    size=self.config.quantity,
                    reason="rp_breakout_confirmed",
                )
            ]

        if bool(current_bar.get("below_rp_confirmed")) and long_open:
            self._long_open_by_symbol[context.symbol] = False
            return [
                StrategyDecision(
                    decision_type=StrategyDecisionType.EXIT_LONG,
                    reason="rp_breakdown_confirmed",
                )
            ]

        return []

    def on_finish(self, context: StrategyContext) -> None:
        self.set_long_open(symbol=context.symbol, is_open=False)

    def set_long_open(self, *, symbol: str, is_open: bool) -> None:
        self._long_open_by_symbol[symbol] = is_open

    @classmethod
    def register_cli_args(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--preset", default=None)
        parser.add_argument("--risk-per-trade", type=float, default=0.005)
        parser.add_argument("--entry-lookback", type=int, default=55)
        parser.add_argument("--exit-lookback", type=int, default=20)
        parser.add_argument("--atr-lookback", type=int, default=20)
        parser.add_argument("--stop-atr-multiple", type=float, default=2.0)
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
        parser.add_argument("--htf-interval", default="1d")
        parser.add_argument("--disable-htf-filter", action="store_true")
        parser.add_argument("--htf-entry-lookback", type=int, default=20)
        parser.add_argument("--htf-expansion-bars", type=int, default=3)
        parser.add_argument("--htf-expansion-min-growth", type=float, default=1.05)
        parser.add_argument("--disable-htf-expansion-filter", action="store_true")

    @classmethod
    def _resolve_flags(cls, args: argparse.Namespace) -> dict[str, Any]:
        allow_short = bool(getattr(args, "allow_short", False))
        if getattr(args, "disable_short", False):
            allow_short = False

        trend_ema_window: int | None = args.trend_ema_window
        if getattr(args, "disable_trend_filter", False):
            trend_ema_window = None

        return {
            "allow_short": allow_short,
            "trend_ema_window": trend_ema_window,
            "require_channel_expansion": not getattr(args, "disable_expansion_filter", False),
            "htf_require_channel_expansion": not getattr(args, "disable_htf_expansion_filter", False),
            "use_trend_strength_layering": not getattr(args, "disable_trend_strength_layering", False),
            "use_signal_score_filter": not getattr(args, "disable_signal_score_filter", False),
            "use_rp_chop_filter": bool(getattr(args, "enable_rp_chop_filter", False)),
            "use_rp_signal_quality_sizing": bool(getattr(args, "enable_rp_signal_quality_sizing", False)),
            "use_regime_filter": not getattr(args, "disable_regime_filter", False),
            "use_vol_target_sizing": bool(getattr(args, "enable_vol_target_sizing", False)),
        }

    @classmethod
    def config_from_args(cls, args: argparse.Namespace) -> Any:
        from cta_core.app.turtle_backtest import TurtleConfig

        flags = cls._resolve_flags(args)

        config_kwargs: dict[str, Any] = {
            "entry_lookback": args.entry_lookback,
            "exit_lookback": args.exit_lookback,
            "atr_lookback": args.atr_lookback,
            "initial_capital": args.initial_capital,
            "risk_per_trade": args.risk_per_trade,
            "fee_bps": args.fee_bps,
            "slippage_bps": args.slippage_bps,
            "stop_atr_multiple": args.stop_atr_multiple,
            "max_leverage": args.max_leverage,
            "cooldown_bars": args.cooldown_bars,
            "pullback_window": args.pullback_window,
            "pullback_tolerance_atr": args.pullback_tolerance_atr,
            "breakout_entry_fraction": args.breakout_entry_fraction,
            "expansion_bars": args.expansion_bars,
            "expansion_min_growth": args.expansion_min_growth,
            "strong_trend_threshold": args.strong_trend_threshold,
            "weak_exit_lookback": args.weak_exit_lookback,
            "strong_exit_lookback": args.strong_exit_lookback,
            "weak_pullback_tolerance_atr": args.weak_pullback_tolerance_atr,
            "strong_pullback_tolerance_atr": args.strong_pullback_tolerance_atr,
            "weak_trend_pullback_only": args.weak_trend_pullback_only,
            "weak_trend_no_trade": args.weak_trend_no_trade,
            "add_on_only_after_profit": args.add_on_only_after_profit,
            "addon_min_unrealized_r": args.addon_min_unrealized_r,
            "min_breakout_distance_atr": args.min_breakout_distance_atr,
            "min_breakout_body_atr": args.min_breakout_body_atr,
            "enable_partial_take_profit": args.enable_partial_take_profit,
            "take_profit_r_multiple": args.take_profit_r_multiple,
            "take_profit_fraction": args.take_profit_fraction,
            "min_signal_score_ratio": args.min_signal_score_ratio,
            "min_position_scale": args.min_position_scale,
            "follow_through_bars": args.follow_through_bars,
            "follow_through_max_wait_bars": args.follow_through_max_wait_bars,
            "max_hold_bars": args.max_hold_bars,
            "use_htf_filter": False,
            "htf_entry_lookback": args.htf_entry_lookback,
            "htf_expansion_bars": args.htf_expansion_bars,
            "htf_expansion_min_growth": args.htf_expansion_min_growth,
            "rp_turnover_window": args.rp_turnover_window,
            "rp_base_turnover": args.rp_base_turnover,
            "rp_max_turnover_cap": args.rp_max_turnover_cap,
            "rp_entry_confirm_bars": args.rp_entry_confirm_bars,
            "rp_exit_confirm_bars": args.rp_exit_confirm_bars,
            "rp_entry_band_atr": args.rp_entry_band_atr,
            "rp_exit_band_atr": args.rp_exit_band_atr,
            "rp_min_hold_bars": args.rp_min_hold_bars,
            "rp_htf_slope_bars": args.rp_htf_slope_bars,
            "rp_slope_bars": args.rp_slope_bars,
            "rp_min_slope_ratio": args.rp_min_slope_ratio,
            "rp_min_atr_ratio": args.rp_min_atr_ratio,
            "rp_quality_target_atr": args.rp_quality_target_atr,
            "rp_quality_min_scale": args.rp_quality_min_scale,
            "regime_ema_window": args.regime_ema_window,
            "regime_slope_bars": args.regime_slope_bars,
            "regime_min_slope": args.regime_min_slope,
            "target_annual_vol": args.target_annual_vol,
            "vol_target_window": args.vol_target_window,
            "min_position_allocation": args.min_position_allocation,
            **flags,
        }

        return TurtleConfig.from_flat_kwargs(**config_kwargs)


__all__ = ["RPDailyBreakoutConfig", "RPDailyBreakoutStrategy"]
