from __future__ import annotations

import pytest

from cta_core.app.strategy_presets import (
    BacktestStrategyPreset,
    get_backtest_strategy,
    list_backtest_strategies,
)


def test_registry_contains_rp_live_preset() -> None:
    strategies = list_backtest_strategies()
    ids = [item.strategy_id for item in strategies]
    assert "rp_live" in ids


def test_get_rp_live_has_expected_defaults() -> None:
    preset = get_backtest_strategy("rp_live")
    assert isinstance(preset, BacktestStrategyPreset)
    assert preset.defaults["rp_entry_confirm_bars"] == 3
    assert preset.defaults["rp_exit_confirm_bars"] == 3
    assert preset.defaults["regime_ema_window"] == 30
    assert preset.defaults["regime_min_slope"] == 0.002
    assert preset.defaults["max_hold_bars"] == 40
    assert preset.defaults["use_vol_target_sizing"] is False


def test_merged_defaults_does_not_mutate_original() -> None:
    preset = get_backtest_strategy("rp_live")
    merged = preset.merged_defaults({"max_hold_bars": 20})
    assert merged["max_hold_bars"] == 20
    assert preset.defaults["max_hold_bars"] == 40


def test_get_unknown_strategy_raises_clear_error() -> None:
    with pytest.raises(ValueError, match="unknown strategy"):
        get_backtest_strategy("does_not_exist")
