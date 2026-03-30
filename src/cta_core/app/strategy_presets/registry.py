from __future__ import annotations

from cta_core.app.strategy_presets.base import BacktestStrategyPreset
from cta_core.app.strategy_presets.rp_live import RP_LIVE_PRESET


_STRATEGIES: dict[str, BacktestStrategyPreset] = {
    RP_LIVE_PRESET.strategy_id: RP_LIVE_PRESET,
}


def list_backtest_strategies() -> list[BacktestStrategyPreset]:
    return [_STRATEGIES[key] for key in sorted(_STRATEGIES)]


def get_backtest_strategy(strategy_id: str) -> BacktestStrategyPreset:
    preset = _STRATEGIES.get(strategy_id)
    if preset is None:
        available = ", ".join(sorted(_STRATEGIES))
        raise ValueError(f"unknown strategy '{strategy_id}'. available: {available}")
    return preset
