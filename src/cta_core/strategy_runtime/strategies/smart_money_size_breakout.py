from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from decimal import Decimal
from typing import ClassVar

import polars as pl

from cta_core.strategy_runtime.base import BaseStrategy, StrategyContext, StrategyDecision, StrategyDecisionType


@dataclass(frozen=True)
class SmartMoneySizeBreakoutConfig:
    avg_trade_size_window: int = 96
    size_zscore_threshold: float = 2.0
    min_taker_buy_ratio: float = 0.55
    entry_confirm_buy_ratio_threshold: float = 0.5
    close_to_high_threshold: float = 0.8
    exit_buy_ratio_threshold: float = 0.5
    max_hold_bars: int = 2
    enable_failed_breakout_reversal: bool = False
    reversal_trigger_buy_ratio_threshold: float = 0.5
    reversal_close_location_max: float = 0.5
    reversal_exit_buy_ratio_threshold: float = 0.55
    reversal_max_hold_bars: int = 1
    reversal_stop_buffer_pct: float = 0.001
    quantity: Decimal = Decimal("1")

    def __post_init__(self) -> None:
        if self.avg_trade_size_window < 2:
            raise ValueError("avg_trade_size_window must be >= 2")
        if self.size_zscore_threshold <= 0:
            raise ValueError("size_zscore_threshold must be > 0")
        if not (0 < self.min_taker_buy_ratio <= 1):
            raise ValueError("min_taker_buy_ratio must be in (0, 1]")
        if not (0 <= self.entry_confirm_buy_ratio_threshold <= 1):
            raise ValueError("entry_confirm_buy_ratio_threshold must be in [0, 1]")
        if not (0 <= self.close_to_high_threshold <= 1):
            raise ValueError("close_to_high_threshold must be in [0, 1]")
        if not (0 <= self.exit_buy_ratio_threshold <= 1):
            raise ValueError("exit_buy_ratio_threshold must be in [0, 1]")
        if self.max_hold_bars < 1:
            raise ValueError("max_hold_bars must be >= 1")
        if not (0 <= self.reversal_trigger_buy_ratio_threshold <= 1):
            raise ValueError("reversal_trigger_buy_ratio_threshold must be in [0, 1]")
        if not (0 <= self.reversal_close_location_max <= 1):
            raise ValueError("reversal_close_location_max must be in [0, 1]")
        if not (0 <= self.reversal_exit_buy_ratio_threshold <= 1):
            raise ValueError("reversal_exit_buy_ratio_threshold must be in [0, 1]")
        if self.reversal_max_hold_bars < 1:
            raise ValueError("reversal_max_hold_bars must be >= 1")
        if self.reversal_stop_buffer_pct <= 0:
            raise ValueError("reversal_stop_buffer_pct must be > 0")
        if self.quantity <= 0:
            raise ValueError("quantity must be > 0")


@dataclass
class SmartMoneySizeBreakoutStrategy(BaseStrategy):
    config: SmartMoneySizeBreakoutConfig = field(default_factory=SmartMoneySizeBreakoutConfig)
    strategy_id: ClassVar[str] = "smart_money_size_breakout"
    _long_open_by_symbol: dict[str, bool] = field(init=False, default_factory=dict, repr=False, compare=False)
    _short_open_by_symbol: dict[str, bool] = field(init=False, default_factory=dict, repr=False, compare=False)

    def prepare_features(self, bars: pl.DataFrame, bars_htf: pl.DataFrame | None = None) -> pl.DataFrame:
        required_columns = ("open", "high", "low", "close", "volume", "trades_count", "taker_buy_base_volume")
        missing_columns = [column for column in required_columns if column not in bars.columns]
        if missing_columns:
            missing = ", ".join(missing_columns)
            raise ValueError(f"bars missing required columns: {missing}")

        high = [float(value) for value in bars.get_column("high").to_list()]
        low = [float(value) for value in bars.get_column("low").to_list()]
        close = [float(value) for value in bars.get_column("close").to_list()]
        volume = [float(value) for value in bars.get_column("volume").to_list()]
        trades_count_raw = [int(value) if value is not None else 0 for value in bars.get_column("trades_count").to_list()]
        taker_buy = [float(value) for value in bars.get_column("taker_buy_base_volume").to_list()]

        avg_trade_size: list[float | None] = []
        for current_volume, trades_count in zip(volume, trades_count_raw):
            if trades_count <= 0:
                avg_trade_size.append(None)
            else:
                avg_trade_size.append(max(current_volume, 0.0) / trades_count)

        size_mean_prev: list[float | None] = [None for _ in avg_trade_size]
        size_std_prev: list[float | None] = [None for _ in avg_trade_size]
        size_zscore: list[float | None] = [None for _ in avg_trade_size]
        taker_buy_ratio: list[float] = []
        close_to_high_location: list[float] = []
        smb_setup_signal: list[bool] = []
        smb_long_signal: list[bool] = []
        smb_exit_signal: list[bool] = []
        smb_reversal_short_signal: list[bool] = []
        smb_short_exit_signal: list[bool] = []

        for index in range(len(avg_trade_size)):
            current_volume = max(volume[index], 0.0)
            buy_volume = max(min(taker_buy[index], current_volume), 0.0)
            buy_ratio = 0.0 if current_volume <= 0 else buy_volume / current_volume
            taker_buy_ratio.append(buy_ratio)
            smb_exit_signal.append(buy_ratio < self.config.exit_buy_ratio_threshold)
            smb_short_exit_signal.append(buy_ratio > self.config.reversal_exit_buy_ratio_threshold)

            bar_range = high[index] - low[index]
            location = 0.5 if bar_range <= 0 else (close[index] - low[index]) / bar_range
            close_to_high_location.append(location)

            if index < self.config.avg_trade_size_window:
                smb_setup_signal.append(False)
                smb_long_signal.append(False)
                smb_reversal_short_signal.append(False)
                continue

            window_values = avg_trade_size[index - self.config.avg_trade_size_window : index]
            history = [value for value in window_values if value is not None]
            if len(history) != self.config.avg_trade_size_window:
                smb_setup_signal.append(False)
                smb_long_signal.append(False)
                smb_reversal_short_signal.append(False)
                continue

            mean = sum(history) / self.config.avg_trade_size_window
            variance = sum((value - mean) ** 2 for value in history) / self.config.avg_trade_size_window
            std = variance**0.5
            size_mean_prev[index] = mean
            size_std_prev[index] = std

            current_size = avg_trade_size[index]
            zscore = None if current_size is None or std <= 0 else (current_size - mean) / std
            size_zscore[index] = zscore

            setup_signal = (
                zscore is not None
                and zscore > self.config.size_zscore_threshold
                and buy_ratio > self.config.min_taker_buy_ratio
                and location > self.config.close_to_high_threshold
            )
            smb_setup_signal.append(bool(setup_signal))

            confirmed = (
                index >= 1
                and bool(smb_setup_signal[index - 1])
                and buy_ratio >= self.config.entry_confirm_buy_ratio_threshold
            )
            smb_long_signal.append(bool(confirmed))
            reversal = (
                index >= 1
                and bool(smb_setup_signal[index - 1])
                and buy_ratio <= self.config.reversal_trigger_buy_ratio_threshold
                and location <= self.config.reversal_close_location_max
            )
            smb_reversal_short_signal.append(bool(reversal))

        return bars.with_columns(
            pl.Series("smb_avg_trade_size", avg_trade_size),
            pl.Series("smb_size_mean_prev", size_mean_prev),
            pl.Series("smb_size_std_prev", size_std_prev),
            pl.Series("smb_size_zscore", size_zscore),
            pl.Series("smb_taker_buy_ratio", taker_buy_ratio),
            pl.Series("smb_close_to_high_location", close_to_high_location),
            pl.Series("smb_setup_signal", smb_setup_signal),
            pl.Series("smb_long_signal", smb_long_signal),
            pl.Series("smb_exit_signal", smb_exit_signal),
            pl.Series("smb_reversal_short_signal", smb_reversal_short_signal),
            pl.Series("smb_short_exit_signal", smb_short_exit_signal),
        )

    def on_start(self, context: StrategyContext) -> None:
        self.set_long_open(symbol=context.symbol, is_open=False)
        self.set_short_open(symbol=context.symbol, is_open=False)

    def on_bar(self, context: StrategyContext) -> list[StrategyDecision]:
        if context.bars.height == 0:
            return []
        long_open = self._long_open_by_symbol.get(context.symbol, False)
        short_open = self._short_open_by_symbol.get(context.symbol, False)
        current_bar = context.current_bar

        if long_open and bool(current_bar.get("smb_exit_signal")):
            self.set_long_open(symbol=context.symbol, is_open=False)
            return [StrategyDecision(decision_type=StrategyDecisionType.EXIT_LONG, reason="buy_ratio_weakened")]
        if short_open and bool(current_bar.get("smb_short_exit_signal")):
            self.set_short_open(symbol=context.symbol, is_open=False)
            return [StrategyDecision(decision_type=StrategyDecisionType.EXIT_SHORT, reason="buy_ratio_rebound")]

        if (not long_open) and (not short_open) and bool(current_bar.get("smb_long_signal")):
            self.set_long_open(symbol=context.symbol, is_open=True)
            return [
                StrategyDecision(
                    decision_type=StrategyDecisionType.ENTER_LONG,
                    size=self.config.quantity,
                    reason="smart_money_size_breakout",
                )
            ]
        if (
            self.config.enable_failed_breakout_reversal
            and (not long_open)
            and (not short_open)
            and bool(current_bar.get("smb_reversal_short_signal"))
        ):
            self.set_short_open(symbol=context.symbol, is_open=True)
            return [
                StrategyDecision(
                    decision_type=StrategyDecisionType.ENTER_SHORT,
                    size=self.config.quantity,
                    reason="failed_breakout_reversal",
                )
            ]
        return []

    def on_finish(self, context: StrategyContext) -> None:
        self.set_long_open(symbol=context.symbol, is_open=False)
        self.set_short_open(symbol=context.symbol, is_open=False)

    def set_long_open(self, *, symbol: str, is_open: bool) -> None:
        self._long_open_by_symbol[symbol] = is_open

    def set_short_open(self, *, symbol: str, is_open: bool) -> None:
        self._short_open_by_symbol[symbol] = is_open

    @classmethod
    def register_cli_args(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--smb-avg-trade-size-window", type=int, default=96)
        parser.add_argument("--smb-size-zscore-threshold", type=float, default=2.0)
        parser.add_argument("--smb-min-taker-buy-ratio", type=float, default=0.55)
        parser.add_argument("--smb-entry-confirm-buy-ratio-threshold", type=float, default=0.5)
        parser.add_argument("--smb-close-to-high-threshold", type=float, default=0.8)
        parser.add_argument("--smb-exit-buy-ratio-threshold", type=float, default=0.5)
        parser.add_argument("--smb-max-hold-bars", type=int, default=2)
        parser.add_argument("--smb-enable-failed-breakout-reversal", action="store_true")
        parser.add_argument("--smb-reversal-trigger-buy-ratio-threshold", type=float, default=0.5)
        parser.add_argument("--smb-reversal-close-location-max", type=float, default=0.5)
        parser.add_argument("--smb-reversal-exit-buy-ratio-threshold", type=float, default=0.55)
        parser.add_argument("--smb-reversal-max-hold-bars", type=int, default=1)
        parser.add_argument("--smb-reversal-stop-buffer-pct", type=float, default=0.001)

    @classmethod
    def config_from_args(cls, args: argparse.Namespace) -> SmartMoneySizeBreakoutConfig:
        return SmartMoneySizeBreakoutConfig(
            avg_trade_size_window=int(args.smb_avg_trade_size_window),
            size_zscore_threshold=float(args.smb_size_zscore_threshold),
            min_taker_buy_ratio=float(args.smb_min_taker_buy_ratio),
            entry_confirm_buy_ratio_threshold=float(args.smb_entry_confirm_buy_ratio_threshold),
            close_to_high_threshold=float(args.smb_close_to_high_threshold),
            exit_buy_ratio_threshold=float(args.smb_exit_buy_ratio_threshold),
            max_hold_bars=int(args.smb_max_hold_bars),
            enable_failed_breakout_reversal=bool(args.smb_enable_failed_breakout_reversal),
            reversal_trigger_buy_ratio_threshold=float(args.smb_reversal_trigger_buy_ratio_threshold),
            reversal_close_location_max=float(args.smb_reversal_close_location_max),
            reversal_exit_buy_ratio_threshold=float(args.smb_reversal_exit_buy_ratio_threshold),
            reversal_max_hold_bars=int(args.smb_reversal_max_hold_bars),
            reversal_stop_buffer_pct=float(args.smb_reversal_stop_buffer_pct),
            quantity=Decimal("1"),
        )


__all__ = ["SmartMoneySizeBreakoutConfig", "SmartMoneySizeBreakoutStrategy"]
