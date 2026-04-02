from __future__ import annotations

import polars as pl

from cta_core.strategy_runtime.base import StrategyContext, StrategyDecisionType
from cta_core.strategy_runtime.strategies.smart_money_size_breakout import (
    SmartMoneySizeBreakoutConfig,
    SmartMoneySizeBreakoutStrategy,
)


def test_entry_and_exit_signal_on_buy_ratio_weakening() -> None:
    bars = pl.DataFrame(
        {
            "open": [100.0, 100.1, 100.0, 100.2, 100.1, 100.0, 100.0, 101.7],
            "high": [100.4, 100.5, 100.3, 100.6, 100.4, 100.2, 101.8, 102.0],
            "low": [99.8, 99.9, 99.8, 100.0, 99.9, 99.7, 99.9, 101.2],
            "close": [100.1, 100.0, 100.2, 100.1, 100.0, 99.9, 101.7, 101.9],
            "volume": [100.0, 98.0, 102.0, 101.0, 99.0, 100.0, 350.0, 150.0],
            "trades_count": [120, 115, 118, 122, 119, 120, 80, 120],
            "taker_buy_base_volume": [52.0, 50.0, 53.0, 52.0, 51.0, 50.0, 230.0, 90.0],
        }
    )
    strategy = SmartMoneySizeBreakoutStrategy(
        SmartMoneySizeBreakoutConfig(
            avg_trade_size_window=5,
            size_zscore_threshold=1.5,
            min_taker_buy_ratio=0.55,
            close_to_high_threshold=0.8,
            exit_buy_ratio_threshold=0.5,
        )
    )
    prepared = strategy.prepare_features(bars)
    strategy.on_start(StrategyContext(symbol="BTCUSDT", bars=prepared))

    enter = strategy.on_bar(StrategyContext(symbol="BTCUSDT", bars=prepared))
    assert [decision.decision_type for decision in enter] == [StrategyDecisionType.ENTER_LONG]

    weakened = prepared.with_columns(
        pl.when(pl.arange(0, prepared.height) == prepared.height - 1)
        .then(pl.lit(True))
        .otherwise(pl.col("smb_exit_signal"))
        .alias("smb_exit_signal")
    )
    exit_decision = strategy.on_bar(StrategyContext(symbol="BTCUSDT", bars=weakened))
    assert [decision.decision_type for decision in exit_decision] == [StrategyDecisionType.EXIT_LONG]


def test_failed_breakout_reversal_short_entry_and_exit() -> None:
    bars = pl.DataFrame(
        {
            "open": [100.0, 100.1, 100.0, 100.2, 100.1, 100.0, 100.0, 101.7],
            "high": [100.4, 100.5, 100.3, 100.6, 100.4, 100.2, 101.8, 102.0],
            "low": [99.8, 99.9, 99.8, 100.0, 99.9, 99.7, 99.9, 100.1],
            "close": [100.1, 100.0, 100.2, 100.1, 100.0, 99.9, 101.7, 100.3],
            "volume": [100.0, 98.0, 102.0, 101.0, 99.0, 100.0, 350.0, 150.0],
            "trades_count": [120, 115, 118, 122, 119, 120, 80, 120],
            "taker_buy_base_volume": [52.0, 50.0, 53.0, 52.0, 51.0, 50.0, 230.0, 60.0],
        }
    )
    strategy = SmartMoneySizeBreakoutStrategy(
        SmartMoneySizeBreakoutConfig(
            avg_trade_size_window=5,
            size_zscore_threshold=1.5,
            min_taker_buy_ratio=0.55,
            entry_confirm_buy_ratio_threshold=0.5,
            close_to_high_threshold=0.8,
            enable_failed_breakout_reversal=True,
            reversal_trigger_buy_ratio_threshold=0.5,
            reversal_close_location_max=0.5,
            reversal_exit_buy_ratio_threshold=0.55,
        )
    )
    prepared = strategy.prepare_features(bars)
    strategy.on_start(StrategyContext(symbol="BTCUSDT", bars=prepared))

    enter_short = strategy.on_bar(StrategyContext(symbol="BTCUSDT", bars=prepared))
    assert [decision.decision_type for decision in enter_short] == [StrategyDecisionType.ENTER_SHORT]

    rebound = prepared.with_columns(
        pl.when(pl.arange(0, prepared.height) == prepared.height - 1)
        .then(pl.lit(True))
        .otherwise(pl.col("smb_short_exit_signal"))
        .alias("smb_short_exit_signal")
    )
    exit_short = strategy.on_bar(StrategyContext(symbol="BTCUSDT", bars=rebound))
    assert [decision.decision_type for decision in exit_short] == [StrategyDecisionType.EXIT_SHORT]
