from __future__ import annotations

import argparse
from dataclasses import dataclass
from decimal import Decimal
from typing import ClassVar

import polars as pl

from cta_core.events import OrderIntent, Side
from cta_core.strategy_runtime.interfaces import Strategy, StrategyContext


@dataclass(frozen=True)
class SmaCrossStrategy(Strategy):
    fast: int
    slow: int
    strategy_id: ClassVar[str] = "sma_cross"

    def __post_init__(self) -> None:
        if self.fast <= 0:
            raise ValueError("fast must be > 0")
        if self.slow <= 0:
            raise ValueError("slow must be > 0")
        if self.fast >= self.slow:
            raise ValueError("fast must be < slow")

    def on_bar_close(self, context: StrategyContext) -> OrderIntent | None:
        if len(context.bars) < self.slow + 1:
            return None

        close = context.bars.get_column("close")
        fast_ma = self._tail_mean(close, self.fast)
        slow_ma = self._tail_mean(close, self.slow)

        if fast_ma > slow_ma:
            return OrderIntent(
                strategy_id=self.strategy_id,
                symbol=context.symbol,
                side=Side.BUY,
                quantity=Decimal("0.01"),
                order_type="MARKET",
            )
        return None

    @staticmethod
    def _tail_mean(close: pl.Series, window: int) -> float:
        return float(close.tail(window).mean())

    @classmethod
    def register_cli_args(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--fast", type=int, default=10)
        parser.add_argument("--slow", type=int, default=20)

    @classmethod
    def config_from_args(cls, args: argparse.Namespace) -> SmaCrossStrategy:
        return cls(fast=args.fast, slow=args.slow)
