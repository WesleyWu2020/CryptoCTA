from __future__ import annotations

from cta_core.app.strategy_presets.base import BacktestStrategyPreset


RP_LIVE_PRESET = BacktestStrategyPreset(
    strategy_id="rp_live",
    description="RP live-ready preset from the latest Sharpe/Calmar-selected parameters.",
    defaults={
        "allow_short": False,
        "use_rp_chop_filter": False,
        "use_rp_signal_quality_sizing": False,
        "rp_entry_confirm_bars": 3,
        "rp_exit_confirm_bars": 3,
        "regime_ema_window": 30,
        "regime_min_slope": 0.002,
        "max_hold_bars": 40,
        "use_vol_target_sizing": False,
    },
)
