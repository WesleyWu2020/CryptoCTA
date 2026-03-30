from __future__ import annotations

from cta_core.app.strategy_presets.base import BacktestStrategyPreset


# Copy this file and replace fields to add a new strategy preset.
TEMPLATE_PRESET = BacktestStrategyPreset(
    strategy_id="template_strategy",
    description="Template for adding a backtest strategy preset.",
    defaults={
        # Example fields accepted by scripts/run_turtle_backtest.py:
        # "interval": "1d",
        # "allow_short": False,
        # "rp_entry_confirm_bars": 3,
    },
)
