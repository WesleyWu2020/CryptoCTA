import polars as pl

from cta_core.strategy_runtime.runtime import run_bar_close
from cta_core.strategy_runtime.strategies.sma_cross import SmaCrossStrategy


def test_sma_cross_emits_order_intent():
    bars = pl.DataFrame({"close": [10.0, 10.5, 11.0, 12.0, 13.0]})
    strategy = SmaCrossStrategy(fast=2, slow=3)
    intent = run_bar_close(strategy=strategy, bars=bars, symbol="BTCUSDT")
    assert intent is not None
    assert intent.symbol == "BTCUSDT"
