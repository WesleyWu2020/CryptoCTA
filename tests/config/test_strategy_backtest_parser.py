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
            "allow_short": True,
            "max_hold_bars": 12,
            "use_rp_chop_filter": True,
            "use_rp_signal_quality_sizing": True,
            "use_vol_target_sizing": True,
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
    assert args.allow_short is True
    assert args.max_hold_bars == 12
    assert args.use_rp_chop_filter is True
    assert args.use_rp_signal_quality_sizing is True
    assert args.use_vol_target_sizing is True
    assert config.entry_confirmations == 5
    assert config.exit_confirmations == 4
