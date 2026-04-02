from __future__ import annotations

import polars as pl

from cta_core.strategy_runtime.strategies.liquidation_vacuum_reversion import (
    LiquidationVacuumReversionConfig,
    LiquidationVacuumReversionStrategy,
)


def test_prepare_features_flags_vacuum_rebound_long_signal() -> None:
    bars = pl.DataFrame(
        {
            "open": [100.0, 99.8, 100.2, 100.0, 99.9, 99.7],
            "high": [100.5, 100.3, 100.7, 100.4, 100.2, 99.9],
            "low": [99.6, 99.3, 99.7, 99.5, 99.3, 97.8],
            "close": [99.9, 99.7, 100.1, 99.8, 99.6, 98.8],
            "volume": [100.0, 105.0, 95.0, 102.0, 108.0, 240.0],
            "taker_buy_base_volume": [48.0, 50.0, 46.0, 49.0, 50.0, 50.0],
        }
    )
    strategy = LiquidationVacuumReversionStrategy(
        LiquidationVacuumReversionConfig(
            volume_peak_window=5,
            min_range_pct=0.015,
            min_taker_sell_ratio=0.7,
        )
    )

    prepared = strategy.prepare_features(bars)
    assert bool(prepared.get_column("lvr_long_signal").to_list()[-1]) is True
