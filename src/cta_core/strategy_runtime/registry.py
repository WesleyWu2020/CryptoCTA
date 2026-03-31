from __future__ import annotations

from cta_core.strategy_runtime.strategies.rp_daily_breakout import RPDailyBreakoutConfig, RPDailyBreakoutStrategy
from cta_core.strategy_runtime.strategies.sma_cross import SmaCrossStrategy


_STRATEGY_CLASSES: dict[str, type] = {
    "rp_daily_breakout": RPDailyBreakoutStrategy,
    "sma_cross": SmaCrossStrategy,
}


def list_strategy_ids() -> list[str]:
    return list(_STRATEGY_CLASSES)


def get_strategy_class(strategy_id: str) -> type:
    try:
        return _STRATEGY_CLASSES[strategy_id]
    except KeyError as error:
        raise ValueError(f"unknown strategy_id: {strategy_id}") from error


def build_strategy(strategy_id: str) -> RPDailyBreakoutStrategy | SmaCrossStrategy:
    cls = get_strategy_class(strategy_id)
    if cls is RPDailyBreakoutStrategy:
        return RPDailyBreakoutStrategy(RPDailyBreakoutConfig())
    if cls is SmaCrossStrategy:
        return SmaCrossStrategy(fast=10, slow=20)
    raise ValueError(f"unknown strategy_id: {strategy_id}")
