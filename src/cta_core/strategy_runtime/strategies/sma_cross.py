from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import polars as pl

from cta_core.events import OrderIntent, Side
from cta_core.strategy_runtime.interfaces import Strategy, StrategyContext


@dataclass(frozen=True)
class SmaCrossStrategy(Strategy):
    fast: int
    slow: int
    strategy_id: str = "sma_cross"

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
