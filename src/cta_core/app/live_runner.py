from __future__ import annotations

from decimal import Decimal

from cta_core.events import OrderIntent, Side
from cta_core.execution.live_binance import LiveBinanceAdapter
from cta_core.app.live_config import LiveRunConfig
from cta_core.risk import RiskContext, RiskEngine, RiskResult
from cta_core.strategy_runtime import StrategyDecision, StrategyDecisionType


def bootstrap_live_runner(api_key: str, api_secret: str) -> LiveBinanceAdapter:
    return LiveBinanceAdapter(api_key=api_key, api_secret=api_secret)


def decision_to_intent(strategy_id: str, symbol: str, decision: StrategyDecision) -> OrderIntent | None:
    if decision.decision_type == StrategyDecisionType.ENTER_LONG:
        return OrderIntent(
            strategy_id=strategy_id,
            symbol=symbol,
            side=Side.BUY,
            quantity=decision.size,
            order_type="MARKET",
        )
    if decision.decision_type == StrategyDecisionType.EXIT_LONG:
        quantity = decision.size if decision.size > Decimal("0") else Decimal("0")
        return OrderIntent(
            strategy_id=strategy_id,
            symbol=symbol,
            side=Side.SELL,
            quantity=quantity,
            order_type="MARKET",
        )
    return None


def check_risk(engine: RiskEngine, ctx: RiskContext) -> RiskResult:
    return engine.check(ctx)


def main(argv: list[str] | None = None) -> int:
    config = LiveRunConfig.from_argv(argv)
    if config.dry_run:
        return 0

    bootstrap_live_runner(api_key=config.api_key, api_secret=config.api_secret)
    return 0


__all__ = ["bootstrap_live_runner", "check_risk", "decision_to_intent", "main"]
