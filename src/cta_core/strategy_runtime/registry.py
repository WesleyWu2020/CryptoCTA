from __future__ import annotations

from cta_core.strategy_runtime.strategies.liquidity_shock_reversion import (
    LiquidityShockReversionConfig,
    LiquidityShockReversionStrategy,
)
from cta_core.strategy_runtime.strategies.liquidation_vacuum_reversion import (
    LiquidationVacuumReversionConfig,
    LiquidationVacuumReversionStrategy,
)
from cta_core.strategy_runtime.strategies.rp_daily_breakout import RPDailyBreakoutConfig, RPDailyBreakoutStrategy
from cta_core.strategy_runtime.strategies.rsi_threshold import RSIThresholdConfig, RSIThresholdStrategy
from cta_core.strategy_runtime.strategies.smart_money_size_breakout import (
    SmartMoneySizeBreakoutConfig,
    SmartMoneySizeBreakoutStrategy,
)
from cta_core.strategy_runtime.strategies.sma_cross import SmaCrossStrategy
from cta_core.strategy_runtime.strategies.taker_imbalance_absorption import (
    TakerImbalanceAbsorptionConfig,
    TakerImbalanceAbsorptionStrategy,
)


_STRATEGY_CLASSES: dict[str, type] = {
    "rp_daily_breakout": RPDailyBreakoutStrategy,
    "rsi_threshold": RSIThresholdStrategy,
    "sma_cross": SmaCrossStrategy,
    "liquidity_shock_reversion": LiquidityShockReversionStrategy,
    "taker_imbalance_absorption": TakerImbalanceAbsorptionStrategy,
    "liquidation_vacuum_reversion": LiquidationVacuumReversionStrategy,
    "smart_money_size_breakout": SmartMoneySizeBreakoutStrategy,
}


def list_strategy_ids() -> list[str]:
    return list(_STRATEGY_CLASSES)


def get_strategy_class(strategy_id: str) -> type:
    try:
        return _STRATEGY_CLASSES[strategy_id]
    except KeyError as error:
        raise ValueError(f"unknown strategy_id: {strategy_id}") from error


def build_strategy(
    strategy_id: str,
) -> (
    RPDailyBreakoutStrategy
    | RSIThresholdStrategy
    | SmaCrossStrategy
    | LiquidityShockReversionStrategy
    | TakerImbalanceAbsorptionStrategy
    | LiquidationVacuumReversionStrategy
    | SmartMoneySizeBreakoutStrategy
):
    cls = get_strategy_class(strategy_id)
    if cls is RPDailyBreakoutStrategy:
        return RPDailyBreakoutStrategy(RPDailyBreakoutConfig())
    if cls is RSIThresholdStrategy:
        return RSIThresholdStrategy(RSIThresholdConfig())
    if cls is SmaCrossStrategy:
        return SmaCrossStrategy(fast=10, slow=20)
    if cls is LiquidityShockReversionStrategy:
        return LiquidityShockReversionStrategy(LiquidityShockReversionConfig())
    if cls is TakerImbalanceAbsorptionStrategy:
        return TakerImbalanceAbsorptionStrategy(TakerImbalanceAbsorptionConfig())
    if cls is LiquidationVacuumReversionStrategy:
        return LiquidationVacuumReversionStrategy(LiquidationVacuumReversionConfig())
    if cls is SmartMoneySizeBreakoutStrategy:
        return SmartMoneySizeBreakoutStrategy(SmartMoneySizeBreakoutConfig())
    raise ValueError(f"unknown strategy_id: {strategy_id}")
