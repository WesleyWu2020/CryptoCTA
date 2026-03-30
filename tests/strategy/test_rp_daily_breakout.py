from __future__ import annotations

from decimal import Decimal

import polars as pl

from cta_core.strategy_runtime.base import StrategyContext, StrategyDecisionType
from cta_core.strategy_runtime.strategies import RPDailyBreakoutConfig, RPDailyBreakoutStrategy


def _run_strategy(strategy: RPDailyBreakoutStrategy, bars: pl.DataFrame) -> list[list[str]]:
    prepared = strategy.prepare_features(bars)
    strategy.on_start(StrategyContext(symbol="BTCUSDT", bars=prepared))
    decisions_by_bar: list[list[str]] = []
    for index in range(prepared.height):
        context = StrategyContext(symbol="BTCUSDT", bars=prepared.head(index + 1))
        decisions_by_bar.append([decision.decision_type.value for decision in strategy.on_bar(context)])
    strategy.on_finish(StrategyContext(symbol="BTCUSDT", bars=prepared))
    return decisions_by_bar


def test_prepare_features_attaches_rp_proxy_and_confirmation_columns() -> None:
    bars = pl.DataFrame({"close": [10.0, 11.0, 12.0, 13.0]})
    strategy = RPDailyBreakoutStrategy(RPDailyBreakoutConfig())

    prepared = strategy.prepare_features(bars)

    assert {"rp", "close_above_rp", "close_below_rp", "above_rp_streak", "below_rp_streak", "above_rp_confirmed", "below_rp_confirmed"}.issubset(
        set(prepared.columns)
    )
    assert prepared["rp"].to_list()[:4] == [10.0, 10.0, 10.5, 11.0]
    assert prepared["above_rp_confirmed"].to_list()[:4] == [False, False, True, True]
    assert prepared["below_rp_confirmed"].to_list()[:4] == [False, False, False, False]


def test_emits_entry_after_configured_confirmed_closes_above_rp() -> None:
    bars = pl.DataFrame({"close": [10.0, 11.0, 12.0, 13.0, 12.0]})
    strategy = RPDailyBreakoutStrategy(
        RPDailyBreakoutConfig(rp_window=2, entry_confirmations=2, exit_confirmations=2, quantity=Decimal("1"))
    )

    decisions_by_bar = _run_strategy(strategy, bars)

    assert decisions_by_bar == [[], [], [StrategyDecisionType.ENTER_LONG.value], [], []]


def test_emits_exit_after_configured_confirmed_closes_below_rp_while_long() -> None:
    bars = pl.DataFrame({"close": [10.0, 11.0, 12.0, 13.0, 12.0, 11.0, 10.0]})
    strategy = RPDailyBreakoutStrategy(
        RPDailyBreakoutConfig(rp_window=2, entry_confirmations=2, exit_confirmations=2, quantity=Decimal("1"))
    )

    decisions_by_bar = _run_strategy(strategy, bars)

    assert decisions_by_bar == [
        [],
        [],
        [StrategyDecisionType.ENTER_LONG.value],
        [],
        [],
        [StrategyDecisionType.EXIT_LONG.value],
        [],
    ]


def test_does_not_repeat_entry_for_same_sliding_window_context_when_state_is_open() -> None:
    bars = pl.DataFrame({"close": [9.0, 10.0]})
    strategy = RPDailyBreakoutStrategy(
        RPDailyBreakoutConfig(rp_window=1, entry_confirmations=1, exit_confirmations=1, quantity=Decimal("1"))
    )
    prepared = strategy.prepare_features(bars)
    context = StrategyContext(symbol="BTCUSDT", bars=prepared.head(2))

    strategy.on_start(context)
    first = [decision.decision_type.value for decision in strategy.on_bar(context)]
    second = [decision.decision_type.value for decision in strategy.on_bar(context)]
    strategy.on_finish(context)

    assert first == [StrategyDecisionType.ENTER_LONG.value]
    assert second == []


def test_state_is_isolated_per_symbol_on_same_strategy_instance() -> None:
    bars = pl.DataFrame({"close": [9.0, 10.0]})
    strategy = RPDailyBreakoutStrategy(
        RPDailyBreakoutConfig(rp_window=1, entry_confirmations=1, exit_confirmations=1, quantity=Decimal("1"))
    )
    prepared = strategy.prepare_features(bars)

    symbol_a = StrategyContext(symbol="BTCUSDT", bars=prepared.head(2))
    symbol_b = StrategyContext(symbol="ETHUSDT", bars=prepared.head(2))

    strategy.on_start(symbol_a)
    first = [decision.decision_type.value for decision in strategy.on_bar(symbol_a)]
    second = [decision.decision_type.value for decision in strategy.on_bar(symbol_b)]

    assert first == [StrategyDecisionType.ENTER_LONG.value]
    assert second == [StrategyDecisionType.ENTER_LONG.value]


def test_on_start_clears_stale_state_for_same_symbol_across_runs() -> None:
    bars = pl.DataFrame({"close": [9.0, 10.0]})
    strategy = RPDailyBreakoutStrategy(
        RPDailyBreakoutConfig(rp_window=1, entry_confirmations=1, exit_confirmations=1, quantity=Decimal("1"))
    )
    prepared = strategy.prepare_features(bars)
    context = StrategyContext(symbol="BTCUSDT", bars=prepared.head(2))

    strategy._long_open_by_symbol["BTCUSDT"] = True
    strategy.on_start(context)

    decisions = [decision.decision_type.value for decision in strategy.on_bar(context)]

    assert decisions == [StrategyDecisionType.ENTER_LONG.value]
