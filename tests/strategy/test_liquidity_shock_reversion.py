from __future__ import annotations

from decimal import Decimal

import polars as pl

from cta_core.strategy_runtime.base import StrategyContext, StrategyDecisionType
from cta_core.strategy_runtime.strategies.liquidity_shock_reversion import (
    LiquidityShockReversionConfig,
    LiquidityShockReversionStrategy,
)


def test_prepare_features_flags_extreme_downside_reversion_signal() -> None:
    bars = pl.DataFrame(
        {
            "open": [100.0, 101.0, 100.0, 101.0, 100.0, 101.0, 92.0, 94.0],
            "high": [101.0, 102.0, 101.0, 102.0, 101.0, 102.0, 103.0, 95.0],
            "low": [99.0, 100.0, 99.0, 100.0, 99.0, 100.0, 80.0, 91.0],
            "close": [100.0, 100.5, 100.0, 100.4, 100.1, 100.2, 92.0, 93.5],
            "volume": [100.0, 105.0, 98.0, 102.0, 99.0, 101.0, 1000.0, 150.0],
        }
    )
    strategy = LiquidityShockReversionStrategy(
        LiquidityShockReversionConfig(
            lookback_bars=4,
            zscore_threshold=2.0,
            volume_quantile=0.95,
            quantity=Decimal("1"),
        )
    )

    prepared = strategy.prepare_features(bars)

    assert bool(prepared.get_column("mr_long_signal").to_list()[6]) is True
    assert bool(prepared.get_column("mr_short_signal").to_list()[6]) is False


def test_prepare_features_flags_extreme_upside_reversion_signal() -> None:
    bars = pl.DataFrame(
        {
            "open": [100.0, 99.5, 100.0, 99.7, 100.2, 100.0, 100.0, 105.5],
            "high": [101.0, 100.0, 101.0, 100.2, 100.7, 100.5, 114.0, 106.0],
            "low": [99.0, 98.8, 99.1, 99.0, 99.6, 99.5, 99.0, 104.0],
            "close": [100.0, 99.8, 100.1, 99.9, 100.0, 100.2, 106.0, 105.0],
            "volume": [100.0, 102.0, 98.0, 101.0, 99.0, 100.0, 1200.0, 160.0],
        }
    )
    strategy = LiquidityShockReversionStrategy(
        LiquidityShockReversionConfig(
            lookback_bars=4,
            zscore_threshold=2.0,
            volume_quantile=0.95,
            quantity=Decimal("1"),
        )
    )

    prepared = strategy.prepare_features(bars)

    assert bool(prepared.get_column("mr_short_signal").to_list()[6]) is True
    assert bool(prepared.get_column("mr_long_signal").to_list()[6]) is False


def test_on_bar_emits_single_entry_when_flat_only() -> None:
    bars = pl.DataFrame(
        {
            "open": [100.0, 101.0, 100.0, 101.0, 100.0, 101.0, 92.0],
            "high": [101.0, 102.0, 101.0, 102.0, 101.0, 102.0, 103.0],
            "low": [99.0, 100.0, 99.0, 100.0, 99.0, 100.0, 80.0],
            "close": [100.0, 100.5, 100.0, 100.4, 100.1, 100.2, 92.0],
            "volume": [100.0, 105.0, 98.0, 102.0, 99.0, 101.0, 1000.0],
        }
    )
    strategy = LiquidityShockReversionStrategy(
        LiquidityShockReversionConfig(lookback_bars=4, zscore_threshold=2.0, volume_quantile=0.95)
    )
    prepared = strategy.prepare_features(bars)
    strategy.on_start(StrategyContext(symbol="BTCUSDT", bars=prepared))

    enter = strategy.on_bar(StrategyContext(symbol="BTCUSDT", bars=prepared))
    assert [decision.decision_type for decision in enter] == [StrategyDecisionType.ENTER_LONG]

    none_when_open = strategy.on_bar(StrategyContext(symbol="BTCUSDT", bars=prepared))
    assert none_when_open == []


def test_dynamic_zscore_can_unlock_signal_when_static_threshold_blocks() -> None:
    bars = pl.DataFrame(
        {
            "open": [100.0, 104.0, 99.84, 100.3392, 99.837504, 99.33831648, 92.38463432, 93.2],
            "high": [100.5, 104.5, 100.2, 100.8, 100.3, 99.9, 99.0, 93.6],
            "low": [99.5, 103.5, 99.2, 99.8, 99.3, 98.8, 80.0, 92.6],
            "close": [100.0, 104.0, 99.84, 100.3392, 99.837504, 99.33831648, 92.38463432, 92.9],
            "volume": [100.0, 120.0, 115.0, 110.0, 108.0, 107.0, 1300.0, 150.0],
        }
    )
    static_strategy = LiquidityShockReversionStrategy(
        LiquidityShockReversionConfig(
            lookback_bars=4,
            zscore_threshold=3.5,
            volume_quantile=0.9,
            quantity=Decimal("1"),
        )
    )
    dynamic_strategy = LiquidityShockReversionStrategy(
        LiquidityShockReversionConfig(
            lookback_bars=4,
            zscore_threshold=3.5,
            volume_quantile=0.9,
            use_dynamic_zscore_threshold=True,
            dynamic_zscore_lookback=2,
            dynamic_zscore_min_scale=0.7,
            dynamic_zscore_max_scale=1.3,
            quantity=Decimal("1"),
        )
    )

    static_prepared = static_strategy.prepare_features(bars)
    dynamic_prepared = dynamic_strategy.prepare_features(bars)

    assert bool(static_prepared.get_column("mr_long_signal").to_list()[6]) is False
    assert bool(dynamic_prepared.get_column("mr_long_signal").to_list()[6]) is True
