from __future__ import annotations

from cta_core.app.strategy_backtest.parser import parse_args
from cta_core.app.strategy_presets.base import BacktestStrategyPreset
from cta_core.strategy_runtime.strategies.rp_daily_breakout import RPDailyBreakoutConfig, RPDailyBreakoutStrategy


def test_parse_args_applies_rp_preset_defaults(monkeypatch) -> None:
    from cta_core import app as app_pkg

    preset = BacktestStrategyPreset(
        strategy_id="test_preset",
        description="test",
        defaults={
            "rp_entry_confirm_bars": 5,
            "rp_exit_confirm_bars": 4,
        },
    )

    monkeypatch.setattr(
        app_pkg.strategy_presets,
        "get_backtest_strategy",
        lambda _: preset,
    )

    args = parse_args(["--strategy", "rp_daily_breakout", "--preset", "test_preset"])
    config = RPDailyBreakoutStrategy.config_from_args(args)

    assert isinstance(config, RPDailyBreakoutConfig)
    assert config.entry_confirmations == 5
    assert config.exit_confirmations == 4
