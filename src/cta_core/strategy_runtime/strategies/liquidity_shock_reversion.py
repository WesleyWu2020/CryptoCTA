from __future__ import annotations

import argparse
import math
from dataclasses import dataclass, field
from decimal import Decimal
from typing import ClassVar

import polars as pl

from cta_core.strategy_runtime.base import BaseStrategy, StrategyContext, StrategyDecision, StrategyDecisionType


def _quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]

    rank = (len(ordered) - 1) * q
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return ordered[lower]
    weight = rank - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


@dataclass(frozen=True)
class LiquidityShockReversionConfig:
    lookback_bars: int = 96
    zscore_threshold: float = 3.0
    long_zscore_threshold: float | None = None
    short_zscore_threshold: float | None = None
    volume_quantile: float = 0.95
    long_volume_quantile: float | None = None
    short_volume_quantile: float | None = None
    long_wick_body_min_ratio: float = 1.0
    short_wick_body_min_ratio: float = 1.0
    use_dynamic_zscore_threshold: bool = False
    dynamic_zscore_lookback: int = 8
    dynamic_zscore_min_scale: float = 0.7
    dynamic_zscore_max_scale: float = 1.3
    max_hold_bars: int = 2
    stop_buffer_pct: float = 0.001
    quantity: Decimal = Decimal("1")

    def __post_init__(self) -> None:
        if self.lookback_bars <= 2:
            raise ValueError("lookback_bars must be > 2")
        if self.zscore_threshold <= 0:
            raise ValueError("zscore_threshold must be > 0")
        if self.long_zscore_threshold is not None and self.long_zscore_threshold <= 0:
            raise ValueError("long_zscore_threshold must be > 0 when provided")
        if self.short_zscore_threshold is not None and self.short_zscore_threshold <= 0:
            raise ValueError("short_zscore_threshold must be > 0 when provided")
        if not (0 < self.volume_quantile < 1):
            raise ValueError("volume_quantile must be in (0, 1)")
        if self.long_volume_quantile is not None and not (0 < self.long_volume_quantile < 1):
            raise ValueError("long_volume_quantile must be in (0, 1) when provided")
        if self.short_volume_quantile is not None and not (0 < self.short_volume_quantile < 1):
            raise ValueError("short_volume_quantile must be in (0, 1) when provided")
        if self.long_wick_body_min_ratio <= 0:
            raise ValueError("long_wick_body_min_ratio must be > 0")
        if self.short_wick_body_min_ratio <= 0:
            raise ValueError("short_wick_body_min_ratio must be > 0")
        if self.dynamic_zscore_lookback < 2:
            raise ValueError("dynamic_zscore_lookback must be >= 2")
        if self.dynamic_zscore_min_scale <= 0:
            raise ValueError("dynamic_zscore_min_scale must be > 0")
        if self.dynamic_zscore_max_scale <= 0:
            raise ValueError("dynamic_zscore_max_scale must be > 0")
        if self.dynamic_zscore_min_scale > self.dynamic_zscore_max_scale:
            raise ValueError("dynamic_zscore_min_scale must be <= dynamic_zscore_max_scale")
        if self.max_hold_bars < 1:
            raise ValueError("max_hold_bars must be >= 1")
        if self.stop_buffer_pct <= 0:
            raise ValueError("stop_buffer_pct must be > 0")
        if self.quantity <= 0:
            raise ValueError("quantity must be > 0")


@dataclass
class LiquidityShockReversionStrategy(BaseStrategy):
    config: LiquidityShockReversionConfig = field(default_factory=LiquidityShockReversionConfig)
    strategy_id: ClassVar[str] = "liquidity_shock_reversion"
    _position_side_by_symbol: dict[str, str | None] = field(init=False, default_factory=dict, repr=False, compare=False)

    def prepare_features(self, bars: pl.DataFrame, bars_htf: pl.DataFrame | None = None) -> pl.DataFrame:
        required_columns = ("open", "high", "low", "close", "volume")
        missing_columns = [column for column in required_columns if column not in bars.columns]
        if missing_columns:
            missing = ", ".join(missing_columns)
            raise ValueError(f"bars missing required columns: {missing}")

        open_price = [float(value) for value in bars.get_column("open").to_list()]
        high = [float(value) for value in bars.get_column("high").to_list()]
        low = [float(value) for value in bars.get_column("low").to_list()]
        close = [float(value) for value in bars.get_column("close").to_list()]
        volume = [float(value) for value in bars.get_column("volume").to_list()]
        size = len(close)

        returns: list[float | None] = [None for _ in range(size)]
        for index in range(1, size):
            prev = close[index - 1]
            if prev == 0:
                continue
            returns[index] = (close[index] - prev) / prev

        ret_mean: list[float | None] = [None for _ in range(size)]
        ret_std: list[float | None] = [None for _ in range(size)]
        ret_zscore: list[float | None] = [None for _ in range(size)]
        long_zscore_threshold: list[float | None] = [None for _ in range(size)]
        short_zscore_threshold: list[float | None] = [None for _ in range(size)]
        long_volume_threshold: list[float | None] = [None for _ in range(size)]
        short_volume_threshold: list[float | None] = [None for _ in range(size)]
        extreme_long_volume: list[bool] = [False for _ in range(size)]
        extreme_short_volume: list[bool] = [False for _ in range(size)]
        lower_wick_gt_body: list[bool] = [False for _ in range(size)]
        upper_wick_gt_body: list[bool] = [False for _ in range(size)]
        mr_long_signal: list[bool] = [False for _ in range(size)]
        mr_short_signal: list[bool] = [False for _ in range(size)]
        long_z_base = (
            self.config.zscore_threshold
            if self.config.long_zscore_threshold is None
            else self.config.long_zscore_threshold
        )
        short_z_base = (
            self.config.zscore_threshold
            if self.config.short_zscore_threshold is None
            else self.config.short_zscore_threshold
        )
        long_volume_q = (
            self.config.volume_quantile
            if self.config.long_volume_quantile is None
            else self.config.long_volume_quantile
        )
        short_volume_q = (
            self.config.volume_quantile
            if self.config.short_volume_quantile is None
            else self.config.short_volume_quantile
        )

        for index in range(size):
            if index < self.config.lookback_bars:
                continue

            ret_window = returns[index - self.config.lookback_bars : index]
            hist_returns = [value for value in ret_window if value is not None]
            if len(hist_returns) != self.config.lookback_bars:
                continue

            mean = sum(hist_returns) / self.config.lookback_bars
            variance = sum((value - mean) ** 2 for value in hist_returns) / self.config.lookback_bars
            std = variance**0.5
            ret_mean[index] = mean
            ret_std[index] = std

            current_ret = returns[index]
            if current_ret is not None and std > 0:
                ret_zscore[index] = (current_ret - mean) / std

            vol_hist = volume[index - self.config.lookback_bars : index]
            long_threshold = _quantile(vol_hist, long_volume_q)
            short_threshold = _quantile(vol_hist, short_volume_q)
            long_volume_threshold[index] = long_threshold
            short_volume_threshold[index] = short_threshold
            extreme_long_volume[index] = long_threshold is not None and volume[index] > long_threshold
            extreme_short_volume[index] = short_threshold is not None and volume[index] > short_threshold

            dynamic_scale = 1.0
            if self.config.use_dynamic_zscore_threshold and std > 0:
                dyn_start = max(0, index - self.config.dynamic_zscore_lookback)
                dyn_window = returns[dyn_start:index]
                dyn_hist = [value for value in dyn_window if value is not None]
                if len(dyn_hist) >= 2:
                    dyn_mean = sum(dyn_hist) / len(dyn_hist)
                    dyn_var = sum((value - dyn_mean) ** 2 for value in dyn_hist) / len(dyn_hist)
                    dyn_std = dyn_var**0.5
                    if dyn_std > 0:
                        dynamic_scale = dyn_std / std
            dynamic_scale = min(self.config.dynamic_zscore_max_scale, dynamic_scale)
            dynamic_scale = max(self.config.dynamic_zscore_min_scale, dynamic_scale)
            long_threshold_z = long_z_base * dynamic_scale
            short_threshold_z = short_z_base * dynamic_scale
            long_zscore_threshold[index] = long_threshold_z
            short_zscore_threshold[index] = short_threshold_z

            body = abs(close[index] - open_price[index])
            lower_wick = min(open_price[index], close[index]) - low[index]
            upper_wick = high[index] - max(open_price[index], close[index])
            lower_wick_gt_body[index] = lower_wick > body * self.config.long_wick_body_min_ratio
            upper_wick_gt_body[index] = upper_wick > body * self.config.short_wick_body_min_ratio

            zscore = ret_zscore[index]
            is_extreme_down = zscore is not None and zscore <= -long_threshold_z
            is_extreme_up = zscore is not None and zscore >= short_threshold_z

            mr_long_signal[index] = (
                bool(is_extreme_down)
                and bool(current_ret is not None and current_ret < 0)
                and extreme_long_volume[index]
                and lower_wick_gt_body[index]
            )
            mr_short_signal[index] = (
                bool(is_extreme_up)
                and bool(current_ret is not None and current_ret > 0)
                and extreme_short_volume[index]
                and upper_wick_gt_body[index]
            )

        return bars.with_columns(
            pl.Series("return", returns),
            pl.Series("ret_mean", ret_mean),
            pl.Series("ret_std", ret_std),
            pl.Series("ret_zscore", ret_zscore),
            pl.Series("long_zscore_threshold", long_zscore_threshold),
            pl.Series("short_zscore_threshold", short_zscore_threshold),
            pl.Series("long_volume_threshold", long_volume_threshold),
            pl.Series("short_volume_threshold", short_volume_threshold),
            pl.Series("extreme_long_volume", extreme_long_volume),
            pl.Series("extreme_short_volume", extreme_short_volume),
            pl.Series("lower_wick_gt_body", lower_wick_gt_body),
            pl.Series("upper_wick_gt_body", upper_wick_gt_body),
            pl.Series("mr_long_signal", mr_long_signal),
            pl.Series("mr_short_signal", mr_short_signal),
        )

    def on_start(self, context: StrategyContext) -> None:
        self.set_position_side(symbol=context.symbol, side=None)

    def on_bar(self, context: StrategyContext) -> list[StrategyDecision]:
        if context.bars.height == 0:
            return []

        position_side = self._position_side_by_symbol.get(context.symbol)
        if position_side is not None:
            return []

        current_bar = context.current_bar
        if bool(current_bar.get("mr_long_signal")):
            self.set_position_side(symbol=context.symbol, side="LONG")
            return [
                StrategyDecision(
                    decision_type=StrategyDecisionType.ENTER_LONG,
                    size=self.config.quantity,
                    reason="extreme_downside_reversion",
                )
            ]
        if bool(current_bar.get("mr_short_signal")):
            self.set_position_side(symbol=context.symbol, side="SHORT")
            return [
                StrategyDecision(
                    decision_type=StrategyDecisionType.ENTER_SHORT,
                    size=self.config.quantity,
                    reason="extreme_upside_reversion",
                )
            ]

        return []

    def on_finish(self, context: StrategyContext) -> None:
        self.set_position_side(symbol=context.symbol, side=None)

    def set_position_side(self, *, symbol: str, side: str | None) -> None:
        self._position_side_by_symbol[symbol] = side

    @classmethod
    def register_cli_args(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--lsr-lookback-bars", type=int, default=96)
        parser.add_argument("--lsr-zscore-threshold", type=float, default=3.0)
        parser.add_argument("--lsr-long-zscore-threshold", type=float, default=None)
        parser.add_argument("--lsr-short-zscore-threshold", type=float, default=None)
        parser.add_argument("--lsr-volume-quantile", type=float, default=0.95)
        parser.add_argument("--lsr-long-volume-quantile", type=float, default=None)
        parser.add_argument("--lsr-short-volume-quantile", type=float, default=None)
        parser.add_argument("--lsr-long-wick-body-min-ratio", type=float, default=1.0)
        parser.add_argument("--lsr-short-wick-body-min-ratio", type=float, default=1.0)
        parser.add_argument("--lsr-use-dynamic-zscore-threshold", action="store_true")
        parser.add_argument("--lsr-dynamic-zscore-lookback", type=int, default=8)
        parser.add_argument("--lsr-dynamic-zscore-min-scale", type=float, default=0.7)
        parser.add_argument("--lsr-dynamic-zscore-max-scale", type=float, default=1.3)
        parser.add_argument("--lsr-max-hold-bars", type=int, default=2)
        parser.add_argument("--lsr-stop-buffer-pct", type=float, default=0.001)
        parser.add_argument("--lsr-stop-mode", choices=("bar_extreme", "atr"), default="bar_extreme")
        parser.add_argument("--lsr-atr-window", type=int, default=14)
        parser.add_argument("--lsr-atr-stop-multiplier", type=float, default=0.6)
        parser.add_argument("--lsr-enable-trailing-stop", action="store_true")
        parser.add_argument("--lsr-atr-trailing-multiplier", type=float, default=0.8)
        parser.add_argument("--lsr-enable-partial-take-profit", action="store_true")
        parser.add_argument("--lsr-take-profit-atr-multiple", type=float, default=0.8)
        parser.add_argument("--lsr-take-profit-fraction", type=float, default=0.5)

    @classmethod
    def config_from_args(cls, args: argparse.Namespace) -> LiquidityShockReversionConfig:
        return LiquidityShockReversionConfig(
            lookback_bars=int(args.lsr_lookback_bars),
            zscore_threshold=float(args.lsr_zscore_threshold),
            long_zscore_threshold=args.lsr_long_zscore_threshold,
            short_zscore_threshold=args.lsr_short_zscore_threshold,
            volume_quantile=float(args.lsr_volume_quantile),
            long_volume_quantile=args.lsr_long_volume_quantile,
            short_volume_quantile=args.lsr_short_volume_quantile,
            long_wick_body_min_ratio=float(args.lsr_long_wick_body_min_ratio),
            short_wick_body_min_ratio=float(args.lsr_short_wick_body_min_ratio),
            use_dynamic_zscore_threshold=bool(args.lsr_use_dynamic_zscore_threshold),
            dynamic_zscore_lookback=int(args.lsr_dynamic_zscore_lookback),
            dynamic_zscore_min_scale=float(args.lsr_dynamic_zscore_min_scale),
            dynamic_zscore_max_scale=float(args.lsr_dynamic_zscore_max_scale),
            max_hold_bars=int(args.lsr_max_hold_bars),
            stop_buffer_pct=float(args.lsr_stop_buffer_pct),
            quantity=Decimal("1"),
        )


__all__ = ["LiquidityShockReversionConfig", "LiquidityShockReversionStrategy"]
