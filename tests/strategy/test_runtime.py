from decimal import Decimal

import polars as pl

from cta_core.events import Side
from cta_core.strategy_runtime.runtime import run_bar_close
from cta_core.strategy_runtime.strategies.sma_cross import SmaCrossStrategy


def test_sma_cross_emits_order_intent():
    bars = pl.DataFrame({"close": [10.0, 10.5, 11.0, 12.0, 13.0]})
    strategy = SmaCrossStrategy(fast=2, slow=3)
    intent = run_bar_close(strategy=strategy, bars=bars, symbol="BTCUSDT")
    assert intent is not None
    assert intent.symbol == "BTCUSDT"
    assert intent.side == Side.BUY
    assert intent.order_type == "MARKET"
    assert intent.quantity == Decimal("0.01")


def test_sma_cross_returns_none_when_bars_are_insufficient():
    bars = pl.DataFrame({"close": [10.0, 10.5, 11.0]})
    strategy = SmaCrossStrategy(fast=2, slow=3)
    intent = run_bar_close(strategy=strategy, bars=bars, symbol="BTCUSDT")
    assert intent is None


def test_sma_cross_returns_none_when_fast_is_not_above_slow():
    bars = pl.DataFrame({"close": [13.0, 12.0, 11.0, 10.0, 9.0]})
    strategy = SmaCrossStrategy(fast=2, slow=3)
    intent = run_bar_close(strategy=strategy, bars=bars, symbol="BTCUSDT")
    assert intent is None


def test_sma_cross_rejects_non_positive_windows():
    for kwargs in ({"fast": 0, "slow": 3}, {"fast": 2, "slow": 0}, {"fast": 3, "slow": 3}):
        try:
            SmaCrossStrategy(**kwargs)
        except ValueError:
            pass
        else:
            raise AssertionError(f"expected ValueError for {kwargs}")
