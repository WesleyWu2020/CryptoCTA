from __future__ import annotations

from collections.abc import Callable

from cta_core.strategy_runtime.strategies.rp_daily_breakout import RPDailyBreakoutConfig, RPDailyBreakoutStrategy
from cta_core.strategy_runtime.strategies.sma_cross import SmaCrossStrategy


StrategyFactory = Callable[[], RPDailyBreakoutStrategy | SmaCrossStrategy]

_STRATEGY_FACTORIES: dict[str, StrategyFactory] = {
    "rp_daily_breakout": lambda: RPDailyBreakoutStrategy(RPDailyBreakoutConfig()),
    "sma_cross": lambda: SmaCrossStrategy(fast=10, slow=20),
}


def list_strategy_ids() -> list[str]:
    return list(_STRATEGY_FACTORIES)


def build_strategy(strategy_id: str) -> RPDailyBreakoutStrategy | SmaCrossStrategy:
    try:
        factory = _STRATEGY_FACTORIES[strategy_id]
    except KeyError as error:
        raise ValueError(f"unknown strategy_id: {strategy_id}") from error
    return factory()
