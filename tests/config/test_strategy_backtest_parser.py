from __future__ import annotations

from cta_core.app.strategy_backtest.parser import parse_args
from cta_core.app.strategy_presets.base import BacktestStrategyPreset
from cta_core.strategy_runtime.strategies.rp_daily_breakout import RPDailyBreakoutStrategy


def test_parse_args_maps_use_flag_defaults_from_preset(monkeypatch) -> None:
    from cta_core import app as app_pkg

    preset = BacktestStrategyPreset(
        strategy_id="test_preset",
        description="test",
        defaults={
            "use_regime_filter": False,
            "use_vol_target_sizing": True,
            "use_rp_chop_filter": True,
            "rp_entry_confirm_bars": 5,
        },
    )

    monkeypatch.setattr(
        app_pkg.strategy_presets,
        "get_backtest_strategy",
        lambda _: preset,
    )

    args = parse_args(["--strategy", "rp_daily_breakout", "--preset", "test_preset"])
    config = RPDailyBreakoutStrategy.config_from_args(args)

    assert config.use_regime_filter is False
    assert config.use_vol_target_sizing is True
    assert config.use_rp_chop_filter is True
    assert config.rp_entry_confirm_bars == 5
