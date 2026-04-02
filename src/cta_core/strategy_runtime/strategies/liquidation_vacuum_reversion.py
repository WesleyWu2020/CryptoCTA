from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from decimal import Decimal
from typing import ClassVar

import polars as pl

from cta_core.strategy_runtime.base import BaseStrategy, StrategyContext, StrategyDecision, StrategyDecisionType


def _rolling_max_prev(values: list[float], window: int) -> list[float | None]:
    out: list[float | None] = [None for _ in values]
    for index in range(window, len(values)):
        out[index] = max(values[index - window : index])
    return out


@dataclass(frozen=True)
class LiquidationVacuumReversionConfig:
    volume_peak_window: int = 48
    min_range_pct: float = 0.015
    min_taker_sell_ratio: float = 0.70
    max_hold_bars: int = 2
    quantity: Decimal = Decimal("1")

    def __post_init__(self) -> None:
        if self.volume_peak_window < 2:
            raise ValueError("volume_peak_window must be >= 2")
        if self.min_range_pct <= 0:
            raise ValueError("min_range_pct must be > 0")
        if not (0 < self.min_taker_sell_ratio <= 1):
            raise ValueError("min_taker_sell_ratio must be in (0, 1]")
        if self.max_hold_bars < 1:
            raise ValueError("max_hold_bars must be >= 1")
        if self.quantity <= 0:
            raise ValueError("quantity must be > 0")


@dataclass
class LiquidationVacuumReversionStrategy(BaseStrategy):
    config: LiquidationVacuumReversionConfig = field(default_factory=LiquidationVacuumReversionConfig)
    strategy_id: ClassVar[str] = "liquidation_vacuum_reversion"
    _long_open_by_symbol: dict[str, bool] = field(init=False, default_factory=dict, repr=False, compare=False)

    def prepare_features(self, bars: pl.DataFrame, bars_htf: pl.DataFrame | None = None) -> pl.DataFrame:
        required_columns = ("open", "high", "low", "close", "volume", "taker_buy_base_volume")
        missing_columns = [column for column in required_columns if column not in bars.columns]
        if missing_columns:
            missing = ", ".join(missing_columns)
            raise ValueError(f"bars missing required columns: {missing}")

        open_price = [float(value) for value in bars.get_column("open").to_list()]
        high = [float(value) for value in bars.get_column("high").to_list()]
        low = [float(value) for value in bars.get_column("low").to_list()]
        close = [float(value) for value in bars.get_column("close").to_list()]
        volume = [float(value) for value in bars.get_column("volume").to_list()]
        taker_buy = [float(value) for value in bars.get_column("taker_buy_base_volume").to_list()]

        volume_peak_prev = _rolling_max_prev(volume, self.config.volume_peak_window)
        range_pct: list[float] = []
        taker_sell_ratio: list[float] = []
        lower_wick_positive: list[bool] = []
        lvr_long_signal: list[bool] = []

        for index in range(len(close)):
            current_volume = max(volume[index], 0.0)
            buy_volume = max(min(taker_buy[index], current_volume), 0.0)
            sell_volume = max(current_volume - buy_volume, 0.0)
            sell_ratio = 0.0 if current_volume <= 0 else sell_volume / current_volume
            taker_sell_ratio.append(sell_ratio)

            base_price = abs(open_price[index])
            amplitude = max(high[index] - low[index], 0.0)
            pct = 0.0 if base_price <= 0 else amplitude / base_price
            range_pct.append(pct)

            lower_wick = min(open_price[index], close[index]) - low[index]
            wick_ok = lower_wick > 0
            lower_wick_positive.append(wick_ok)

            volume_ok = volume_peak_prev[index] is not None and current_volume >= float(volume_peak_prev[index])
            range_ok = pct > self.config.min_range_pct
            sell_ok = sell_ratio > self.config.min_taker_sell_ratio
            lvr_long_signal.append(bool(volume_ok and range_ok and sell_ok and wick_ok))

        return bars.with_columns(
            pl.Series("lvr_volume_peak_prev", volume_peak_prev),
            pl.Series("lvr_range_pct", range_pct),
            pl.Series("lvr_taker_sell_ratio", taker_sell_ratio),
            pl.Series("lvr_lower_wick_positive", lower_wick_positive),
            pl.Series("lvr_long_signal", lvr_long_signal),
        )

    def on_start(self, context: StrategyContext) -> None:
        self.set_long_open(symbol=context.symbol, is_open=False)

    def on_bar(self, context: StrategyContext) -> list[StrategyDecision]:
        if context.bars.height == 0:
            return []
        long_open = self._long_open_by_symbol.get(context.symbol, False)
        if bool(context.current_bar.get("lvr_long_signal")) and not long_open:
            self.set_long_open(symbol=context.symbol, is_open=True)
            return [
                StrategyDecision(
                    decision_type=StrategyDecisionType.ENTER_LONG,
                    size=self.config.quantity,
                    reason="sell_liquidation_vacuum_rebound",
                )
            ]
        return []

    def on_finish(self, context: StrategyContext) -> None:
        self.set_long_open(symbol=context.symbol, is_open=False)

    def set_long_open(self, *, symbol: str, is_open: bool) -> None:
        self._long_open_by_symbol[symbol] = is_open

    @classmethod
    def register_cli_args(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--lvr-volume-peak-window", type=int, default=48)
        parser.add_argument("--lvr-min-range-pct", type=float, default=0.015)
        parser.add_argument("--lvr-min-taker-sell-ratio", type=float, default=0.70)
        parser.add_argument("--lvr-max-hold-bars", type=int, default=2)

    @classmethod
    def config_from_args(cls, args: argparse.Namespace) -> LiquidationVacuumReversionConfig:
        return LiquidationVacuumReversionConfig(
            volume_peak_window=int(args.lvr_volume_peak_window),
            min_range_pct=float(args.lvr_min_range_pct),
            min_taker_sell_ratio=float(args.lvr_min_taker_sell_ratio),
            max_hold_bars=int(args.lvr_max_hold_bars),
            quantity=Decimal("1"),
        )


__all__ = ["LiquidationVacuumReversionConfig", "LiquidationVacuumReversionStrategy"]
