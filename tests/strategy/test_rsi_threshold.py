from __future__ import annotations

from decimal import Decimal

import polars as pl

from cta_core.strategy_runtime.base import StrategyContext, StrategyDecisionType
from cta_core.strategy_runtime.strategies.rsi_threshold import RSIThresholdConfig, RSIThresholdStrategy


def _run_strategy(strategy: RSIThresholdStrategy, bars: pl.DataFrame) -> list[list[str]]:
    prepared = strategy.prepare_features(bars)
    strategy.on_start(StrategyContext(symbol="BTCUSDT", bars=prepared))
    decisions_by_bar: list[list[str]] = []
    for index in range(prepared.height):
        context = StrategyContext(symbol="BTCUSDT", bars=prepared.head(index + 1))
        decisions_by_bar.append([decision.decision_type.value for decision in strategy.on_bar(context)])
    strategy.on_finish(StrategyContext(symbol="BTCUSDT", bars=prepared))
    return decisions_by_bar


def test_prepare_features_adds_rsi_column() -> None:
    bars = pl.DataFrame({"close": [100.0, 99.0, 98.0, 97.0, 96.0]})
    strategy = RSIThresholdStrategy(RSIThresholdConfig(rsi_window=2))

    prepared = strategy.prepare_features(bars)

    assert "rsi" in prepared.columns
    assert "ema_fast" in prepared.columns
    assert "ema_slow" in prepared.columns
    assert "trend_long_ok" in prepared.columns
    assert "adx" in prepared.columns
    assert "adx_trend_ok" in prepared.columns
    assert prepared.get_column("rsi").null_count() == 2


def test_enters_below_buy_threshold_and_exits_above_sell_threshold() -> None:
    bars = pl.DataFrame({"close": [100.0, 95.0, 90.0, 92.0, 95.0, 99.0, 105.0]})
    strategy = RSIThresholdStrategy(
        RSIThresholdConfig(
            rsi_window=2,
            buy_threshold=30.0,
            sell_threshold=70.0,
            use_trend_filter=False,
            quantity=Decimal("1"),
        )
    )

    decisions_by_bar = _run_strategy(strategy, bars)

    flattened = [decision for decisions in decisions_by_bar for decision in decisions]
    assert StrategyDecisionType.ENTER_LONG.value in flattened
    assert StrategyDecisionType.EXIT_LONG.value in flattened


def test_trend_filter_blocks_entry_in_downtrend() -> None:
    bars = pl.DataFrame({"close": [120.0, 115.0, 110.0, 105.0, 100.0, 95.0, 90.0, 92.0]})
    strategy = RSIThresholdStrategy(
        RSIThresholdConfig(
            rsi_window=2,
            buy_threshold=30.0,
            sell_threshold=70.0,
            trend_fast_ema_window=2,
            trend_slow_ema_window=5,
            use_trend_filter=True,
            quantity=Decimal("1"),
        )
    )

    decisions_by_bar = _run_strategy(strategy, bars)
    flattened = [decision for decisions in decisions_by_bar for decision in decisions]
    assert StrategyDecisionType.ENTER_LONG.value not in flattened


def test_momentum_mode_enters_above_upper_and_exits_below_lower() -> None:
    bars = pl.DataFrame({"close": [100.0, 104.0, 108.0, 112.0, 109.0, 104.0, 99.0, 95.0]})
    strategy = RSIThresholdStrategy(
        RSIThresholdConfig(
            rsi_window=2,
            buy_threshold=30.0,
            sell_threshold=70.0,
            use_trend_filter=False,
            use_momentum_mode=True,
            quantity=Decimal("1"),
        )
    )

    decisions_by_bar = _run_strategy(strategy, bars)
    flattened = [decision for decisions in decisions_by_bar for decision in decisions]
    assert StrategyDecisionType.ENTER_LONG.value in flattened
    assert StrategyDecisionType.EXIT_LONG.value in flattened


def test_adx_filter_blocks_entries_when_threshold_is_too_high() -> None:
    bars = pl.DataFrame(
        {
            "close": [100.0, 104.0, 108.0, 112.0, 109.0, 104.0, 99.0, 95.0],
            "high": [101.0, 105.0, 109.0, 113.0, 110.0, 105.0, 100.0, 96.0],
            "low": [99.0, 103.0, 107.0, 111.0, 108.0, 103.0, 98.0, 94.0],
        }
    )
    strategy = RSIThresholdStrategy(
        RSIThresholdConfig(
            rsi_window=2,
            buy_threshold=30.0,
            sell_threshold=70.0,
            use_trend_filter=False,
            use_momentum_mode=True,
            use_adx_filter=True,
            adx_window=2,
            adx_threshold=100.0,
            quantity=Decimal("1"),
        )
    )

    decisions_by_bar = _run_strategy(strategy, bars)
    flattened = [decision for decisions in decisions_by_bar for decision in decisions]
    assert StrategyDecisionType.ENTER_LONG.value not in flattened
