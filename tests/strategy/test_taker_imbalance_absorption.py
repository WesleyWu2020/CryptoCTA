from __future__ import annotations

import polars as pl

from cta_core.strategy_runtime.strategies.taker_imbalance_absorption import (
    TakerImbalanceAbsorptionConfig,
    TakerImbalanceAbsorptionStrategy,
)


def test_prepare_features_flags_absorption_short_signal() -> None:
    bars = pl.DataFrame(
        {
            "open": [100.0, 101.0, 101.5, 102.0, 102.2, 102.0],
            "high": [101.0, 102.0, 103.0, 103.5, 104.0, 104.5],
            "low": [99.0, 100.5, 101.0, 101.2, 101.5, 100.5],
            "close": [100.5, 101.4, 102.2, 101.6, 101.7, 101.1],
            "volume": [100.0, 110.0, 105.0, 108.0, 120.0, 220.0],
            "taker_buy_base_volume": [50.0, 60.0, 62.0, 68.0, 70.0, 170.0],
        }
    )
    strategy = TakerImbalanceAbsorptionStrategy(
        TakerImbalanceAbsorptionConfig(
            volume_ma_window=3,
            min_taker_buy_ratio=0.65,
            close_location_max=0.4,
        )
    )

    prepared = strategy.prepare_features(bars)
    assert bool(prepared.get_column("tia_short_signal").to_list()[-1]) is True
