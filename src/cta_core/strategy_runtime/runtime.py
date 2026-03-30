from __future__ import annotations

import polars as pl

from cta_core.events import OrderIntent
from cta_core.strategy_runtime.interfaces import Strategy, StrategyContext


def run_bar_close(*, strategy: Strategy, bars: pl.DataFrame, symbol: str) -> OrderIntent | None:
    context = StrategyContext(symbol=symbol, bars=bars)
    return strategy.on_bar_close(context)
