from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from decimal import Decimal
from typing import ClassVar

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
        parser.add_argument("--rp-window", type=int, default=3)
        parser.add_argument("--entry-confirmations", "--rp-entry-confirm-bars", dest="entry_confirmations", type=int, default=2)
        parser.add_argument("--exit-confirmations", "--rp-exit-confirm-bars", dest="exit_confirmations", type=int, default=2)
        parser.add_argument("--quantity", type=Decimal, default=Decimal("1"))
        parser.add_argument("--allow-short", action="store_true")
        parser.add_argument("--regime-ema-window", type=int, default=30)
        parser.add_argument("--regime-min-slope", type=float, default=0.002)
        parser.add_argument("--max-hold-bars", type=int, default=40)
        parser.add_argument("--use-rp-chop-filter", action="store_true")
        parser.add_argument("--use-rp-signal-quality-sizing", action="store_true")
        parser.add_argument("--use-vol-target-sizing", action="store_true")
        parser.add_argument("--cooldown-bars", type=int, default=4)
        parser.add_argument("--disable-htf-filter", action="store_true")
        parser.add_argument("--htf-interval", default="1d")
        parser.add_argument("--htf-entry-lookback", type=int, default=20)
        parser.add_argument("--htf-expansion-bars", type=int, default=3)
        parser.add_argument("--htf-expansion-min-growth", type=float, default=1.05)
        parser.add_argument("--disable-htf-expansion-filter", action="store_true")

    @classmethod
    def config_from_args(cls, args: argparse.Namespace) -> RPDailyBreakoutConfig:
        return RPDailyBreakoutConfig(
            rp_window=args.rp_window,
            entry_confirmations=args.entry_confirmations,
            exit_confirmations=args.exit_confirmations,
            quantity=args.quantity,
        )


__all__ = ["RPDailyBreakoutConfig", "RPDailyBreakoutStrategy"]
