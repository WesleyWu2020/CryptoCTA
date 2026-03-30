from __future__ import annotations

from decimal import Decimal
from math import inf, nan

import polars as pl
import pytest

from cta_core.strategy_runtime.base import StrategyContext, StrategyDecision, StrategyDecisionType
from cta_core.strategy_runtime.engine import BacktestEngine


class LifecycleStrategy:
    strategy_id = "lifecycle"

    def __init__(self) -> None:
        self.prepared_bars: pl.DataFrame | None = None
        self.started = False
        self.finished = False
        self.bar_lengths: list[int] = []

    def prepare_features(self, bars: pl.DataFrame, bars_htf: pl.DataFrame | None = None) -> pl.DataFrame:
        self.prepared_bars = bars
        return bars

    def on_start(self, context: StrategyContext) -> None:
        self.started = True
        self.start_symbol = context.symbol

    def on_bar(self, context: StrategyContext) -> list[StrategyDecision]:
        self.bar_lengths.append(context.bars.height)
        if context.bars.height == 1:
            return [StrategyDecision(decision_type=StrategyDecisionType.ENTER_LONG, size=Decimal("1"), reason="enter")]
        if context.bars.height == 2:
            return [StrategyDecision(decision_type=StrategyDecisionType.EXIT_LONG, reason="exit")]
        return []

    def on_finish(self, context: StrategyContext) -> None:
        self.finished = True


def test_backtest_engine_replays_bars_sequentially_into_long_trades() -> None:
    bars = pl.DataFrame({"ts_ms": [1_000, 2_000], "close": [100.0, 110.0]})
    strategy = LifecycleStrategy()
    engine = BacktestEngine(
        symbol="BTCUSDT",
        interval="1m",
        initial_equity=Decimal("1000"),
        fee_bps=0,
        slippage_bps=0,
    )

    result = engine.run(strategy=strategy, bars=bars)

    assert strategy.prepared_bars is not None
    assert strategy.started is True
    assert strategy.finished is True
    assert strategy.bar_lengths == [1, 2]
    assert [trade["action"] for trade in result["trades"]] == ["ENTER_LONG", "EXIT_LONG"]
    assert [trade["timestamp"] for trade in result["trades"]] == [1_000, 2_000]
    assert [trade["reason"] for trade in result["trades"]] == ["enter", "exit"]
    assert [trade["price"] for trade in result["trades"]] == [Decimal("100"), Decimal("110")]


def test_backtest_engine_reports_trade_count_in_summary() -> None:
    bars = pl.DataFrame({"ts_ms": [1_000, 2_000], "close": [100.0, 110.0]})
    strategy = LifecycleStrategy()
    engine = BacktestEngine(
        symbol="BTCUSDT",
        interval="1m",
        initial_equity=Decimal("1000"),
        fee_bps=0,
        slippage_bps=0,
    )

    result = engine.run(strategy=strategy, bars=bars)

    assert result["summary"]["trade_count"] == 2


def test_backtest_engine_records_closed_position_quantity_on_exit() -> None:
    bars = pl.DataFrame({"ts_ms": [1_000, 2_000], "close": [100.0, 110.0]})
    strategy = LifecycleStrategy()
    engine = BacktestEngine(
        symbol="BTCUSDT",
        interval="1m",
        initial_equity=Decimal("1000"),
        fee_bps=0,
        slippage_bps=0,
    )

    result = engine.run(strategy=strategy, bars=bars)

    assert result["trades"][1]["quantity"] == Decimal("1")


def test_backtest_engine_rejects_bars_without_price_data() -> None:
    class MissingPriceStrategy:
        strategy_id = "missing-price"

        def prepare_features(self, bars: pl.DataFrame, bars_htf: pl.DataFrame | None = None) -> pl.DataFrame:
            return bars

        def on_start(self, context: StrategyContext) -> None:
            pass

        def on_bar(self, context: StrategyContext) -> list[StrategyDecision]:
            if context.bars.height == 1:
                return [StrategyDecision(decision_type=StrategyDecisionType.ENTER_LONG, size=Decimal("1"))]
            return []

        def on_finish(self, context: StrategyContext) -> None:
            pass

    bars = pl.DataFrame({"ts_ms": [1_000]})
    engine = BacktestEngine(
        symbol="BTCUSDT",
        interval="1m",
        initial_equity=Decimal("1000"),
        fee_bps=0,
        slippage_bps=0,
    )

    try:
        engine.run(strategy=MissingPriceStrategy(), bars=bars)
    except ValueError as exc:
        assert str(exc) == "bar must contain positive close or price"
    else:
        raise AssertionError("expected ValueError for bars without price data")


@pytest.mark.parametrize("bad_price", [nan, inf, -inf])
def test_backtest_engine_rejects_non_finite_price_values(bad_price: float) -> None:
    class NonFinitePriceStrategy:
        strategy_id = "non-finite-price"

        def prepare_features(self, bars: pl.DataFrame, bars_htf: pl.DataFrame | None = None) -> pl.DataFrame:
            return bars

        def on_start(self, context: StrategyContext) -> None:
            pass

        def on_bar(self, context: StrategyContext) -> list[StrategyDecision]:
            if context.bars.height == 1:
                return [StrategyDecision(decision_type=StrategyDecisionType.ENTER_LONG, size=Decimal("1"))]
            return []

        def on_finish(self, context: StrategyContext) -> None:
            pass

    bars = pl.DataFrame({"ts_ms": [1_000], "close": [bad_price]})
    engine = BacktestEngine(
        symbol="BTCUSDT",
        interval="1m",
        initial_equity=Decimal("1000"),
        fee_bps=0,
        slippage_bps=0,
    )

    with pytest.raises(ValueError, match="bar must contain positive close or price"):
        engine.run(strategy=NonFinitePriceStrategy(), bars=bars)
