from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from decimal import Decimal
from typing import ClassVar

import polars as pl

from cta_core.strategy_runtime.base import BaseStrategy, StrategyContext, StrategyDecision, StrategyDecisionType


def _rolling_mean_prev(values: list[float], window: int) -> list[float | None]:
    out: list[float | None] = [None for _ in values]
    for index in range(window, len(values)):
        segment = values[index - window : index]
        out[index] = sum(segment) / window
    return out


@dataclass(frozen=True)
class TakerImbalanceAbsorptionConfig:
    volume_ma_window: int = 20
    min_taker_buy_ratio: float = 0.65
    close_location_max: float = 0.4
    max_hold_bars: int = 2
    quantity: Decimal = Decimal("1")

    def __post_init__(self) -> None:
        if self.volume_ma_window < 2:
            raise ValueError("volume_ma_window must be >= 2")
        if not (0 < self.min_taker_buy_ratio <= 1):
            raise ValueError("min_taker_buy_ratio must be in (0, 1]")
        if not (0 <= self.close_location_max <= 1):
            raise ValueError("close_location_max must be in [0, 1]")
        if self.max_hold_bars < 1:
            raise ValueError("max_hold_bars must be >= 1")
        if self.quantity <= 0:
            raise ValueError("quantity must be > 0")


@dataclass
class TakerImbalanceAbsorptionStrategy(BaseStrategy):
    config: TakerImbalanceAbsorptionConfig = field(default_factory=TakerImbalanceAbsorptionConfig)
    strategy_id: ClassVar[str] = "taker_imbalance_absorption"
    _short_open_by_symbol: dict[str, bool] = field(init=False, default_factory=dict, repr=False, compare=False)

    def prepare_features(self, bars: pl.DataFrame, bars_htf: pl.DataFrame | None = None) -> pl.DataFrame:
        required_columns = ("open", "high", "low", "close", "volume", "taker_buy_base_volume")
        missing_columns = [column for column in required_columns if column not in bars.columns]
        if missing_columns:
            missing = ", ".join(missing_columns)
            raise ValueError(f"bars missing required columns: {missing}")

        high = [float(value) for value in bars.get_column("high").to_list()]
        low = [float(value) for value in bars.get_column("low").to_list()]
        close = [float(value) for value in bars.get_column("close").to_list()]
        volume = [float(value) for value in bars.get_column("volume").to_list()]
        taker_buy = [float(value) for value in bars.get_column("taker_buy_base_volume").to_list()]

        volume_ma_prev = _rolling_mean_prev(volume, self.config.volume_ma_window)
        taker_buy_ratio: list[float] = []
        close_location: list[float] = []
        tia_short_signal: list[bool] = []

        for index in range(len(close)):
            current_volume = max(volume[index], 0.0)
            buy_volume = max(min(taker_buy[index], current_volume), 0.0)
            ratio = 0.0 if current_volume <= 0 else buy_volume / current_volume
            taker_buy_ratio.append(ratio)

            bar_range = high[index] - low[index]
            location = 0.5 if bar_range <= 0 else (close[index] - low[index]) / bar_range
            close_location.append(location)

            volume_ok = volume_ma_prev[index] is not None and current_volume > float(volume_ma_prev[index])
            ratio_ok = ratio > self.config.min_taker_buy_ratio
            close_low_ok = location < self.config.close_location_max
            tia_short_signal.append(bool(volume_ok and ratio_ok and close_low_ok))

        return bars.with_columns(
            pl.Series("tia_volume_ma_prev", volume_ma_prev),
            pl.Series("tia_taker_buy_ratio", taker_buy_ratio),
            pl.Series("tia_close_location", close_location),
            pl.Series("tia_short_signal", tia_short_signal),
        )

    def on_start(self, context: StrategyContext) -> None:
        self.set_short_open(symbol=context.symbol, is_open=False)

    def on_bar(self, context: StrategyContext) -> list[StrategyDecision]:
        if context.bars.height == 0:
            return []
        short_open = self._short_open_by_symbol.get(context.symbol, False)
        current_bar = context.current_bar
        if bool(current_bar.get("tia_short_signal")) and not short_open:
            self.set_short_open(symbol=context.symbol, is_open=True)
            return [
                StrategyDecision(
                    decision_type=StrategyDecisionType.ENTER_SHORT,
                    size=self.config.quantity,
                    reason="taker_buy_absorption_short",
                )
            ]
        return []

    def on_finish(self, context: StrategyContext) -> None:
        self.set_short_open(symbol=context.symbol, is_open=False)

    def set_short_open(self, *, symbol: str, is_open: bool) -> None:
        self._short_open_by_symbol[symbol] = is_open

    @classmethod
    def register_cli_args(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--tia-volume-ma-window", type=int, default=20)
        parser.add_argument("--tia-min-taker-buy-ratio", type=float, default=0.65)
        parser.add_argument("--tia-close-location-max", type=float, default=0.4)
        parser.add_argument("--tia-max-hold-bars", type=int, default=2)

    @classmethod
    def config_from_args(cls, args: argparse.Namespace) -> TakerImbalanceAbsorptionConfig:
        return TakerImbalanceAbsorptionConfig(
            volume_ma_window=int(args.tia_volume_ma_window),
            min_taker_buy_ratio=float(args.tia_min_taker_buy_ratio),
            close_location_max=float(args.tia_close_location_max),
            max_hold_bars=int(args.tia_max_hold_bars),
            quantity=Decimal("1"),
        )


__all__ = ["TakerImbalanceAbsorptionConfig", "TakerImbalanceAbsorptionStrategy"]
