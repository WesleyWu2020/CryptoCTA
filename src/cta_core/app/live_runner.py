from __future__ import annotations

from decimal import Decimal
from typing import Any

import polars as pl

from cta_core.events import OrderIntent, Side
from cta_core.execution.live_binance import LiveBinanceAdapter
from cta_core.app.live_config import LiveRunConfig
from cta_core.risk import RiskContext, RiskEngine, RiskResult
from cta_core.strategy_runtime import BaseStrategy, StrategyContext, StrategyDecision, StrategyDecisionType
from cta_core.strategy_runtime.registry import build_strategy


def bootstrap_live_runner(api_key: str, api_secret: str) -> LiveBinanceAdapter:
    return LiveBinanceAdapter(api_key=api_key, api_secret=api_secret)


def validate_live_mode(*, dry_run: bool, api_key: str, api_secret: str) -> None:
    if dry_run:
        return
    if not api_key or not api_secret:
        raise ValueError("api_key and api_secret are required when dry_run is False")


def decision_to_intent(
    strategy_id: str,
    symbol: str,
    decision: StrategyDecision,
    position_qty: Decimal | None = None,
) -> OrderIntent | None:
    if decision.decision_type == StrategyDecisionType.ENTER_LONG:
        return OrderIntent(
            strategy_id=strategy_id,
            symbol=symbol,
            side=Side.BUY,
            quantity=decision.size,
            order_type="MARKET",
        )
    if decision.decision_type == StrategyDecisionType.EXIT_LONG:
        if decision.size > Decimal("0"):
            quantity = decision.size
        elif position_qty is not None and position_qty > Decimal("0"):
            quantity = position_qty
        else:
            raise ValueError("cannot map EXIT_LONG without positive size/position qty")
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


def run_once(
    *,
    strategy: BaseStrategy,
    adapter: Any,
    bars: pl.DataFrame,
    symbol: str,
    dry_run: bool = False,
    position_qty: Decimal | None = None,
) -> dict[str, Any]:
    if "open_time" not in bars.columns:
        raise ValueError("bars must contain open_time")
    if "close" not in bars.columns:
        raise ValueError("bars must contain close")

    sorted_bars = bars.sort("open_time")
    prepared_bars = strategy.prepare_features(sorted_bars)
    replay_bars = prepared_bars if isinstance(prepared_bars, pl.DataFrame) else sorted_bars

    latest_bar = replay_bars.tail(1)
    latest_context = StrategyContext(symbol=symbol, bars=replay_bars)
    decisions = strategy.on_bar(latest_context)

    submitted_intents: list[dict[str, Any]] = []
    latest_open_time = int(latest_bar.get_column("open_time").item()) if latest_bar.height > 0 else None

    for decision in decisions:
        intent = decision_to_intent(strategy.strategy_id, symbol, decision, position_qty=position_qty)
        if intent is None:
            continue
        if dry_run:
            continue
        response = adapter.submit_order(intent=intent, ts_ms=latest_open_time)
        submitted_intents.append(
            {
                "intent": intent,
                "response": response,
                "decision_type": decision.decision_type.value,
            }
        )

    return {
        "strategy_id": strategy.strategy_id,
        "symbol": symbol,
        "dry_run": dry_run,
        "latest_open_time": latest_open_time,
        "decisions_count": len(decisions),
        "submit_count": len(submitted_intents),
        "decisions": [
            {
                "decision_type": decision.decision_type.value,
                "size": decision.size,
                "reason": decision.reason,
            }
            for decision in decisions
        ],
        "submitted_intents": submitted_intents,
    }


def main(argv: list[str] | None = None) -> int:
    config = LiveRunConfig.from_argv(argv)
    validate_live_mode(dry_run=config.dry_run, api_key=config.api_key, api_secret=config.api_secret)
    strategy = build_strategy(config.strategy_id)
    bootstrap_live_runner(api_key=config.api_key, api_secret=config.api_secret)
    print(f"live_runner startup strategy={strategy.strategy_id} symbol={config.symbol} dry_run={config.dry_run}")
    return 0


__all__ = [
    "bootstrap_live_runner",
    "check_risk",
    "decision_to_intent",
    "main",
    "run_once",
    "validate_live_mode",
]
