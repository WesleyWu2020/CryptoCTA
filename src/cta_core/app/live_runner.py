from __future__ import annotations

import time
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import polars as pl

from cta_core.app.live_config import LiveRunConfig
from cta_core.app.live_data import fetch_closed_bars, select_new_closed_bars
from cta_core.app.live_state import LiveRuntimeState, load_live_state, save_live_state
from cta_core.data.binance_client import BinanceUMClient
from cta_core.events import OrderIntent, Side
from cta_core.execution.live_binance import LiveBinanceAdapter
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
    latest_price: Decimal | None = None,
    equity: Decimal | None = None,
    max_leverage: Decimal = Decimal("1"),
    fee_bps: Decimal = Decimal("0"),
) -> OrderIntent | None:
    if decision.decision_type == StrategyDecisionType.ENTER_LONG:
        if latest_price is None or equity is None:
            raise ValueError("ENTER_LONG requires latest_price and equity for leverage-scaled sizing")
        if latest_price <= Decimal("0"):
            raise ValueError("latest_price must be > 0")
        if equity <= Decimal("0"):
            raise ValueError("equity must be > 0")
        if max_leverage <= Decimal("0"):
            raise ValueError("max_leverage must be > 0")
        if fee_bps < Decimal("0"):
            raise ValueError("fee_bps must be >= 0")

        fee_rate = fee_bps / Decimal("10000")
        denominator = latest_price * (Decimal("1") + fee_rate)
        if denominator <= Decimal("0"):
            raise ValueError("invalid denominator while mapping ENTER_LONG quantity")
        quantity = equity * max_leverage * decision.size / denominator
        if quantity <= Decimal("0"):
            raise ValueError("ENTER_LONG mapped quantity must be > 0")

        return OrderIntent(
            strategy_id=strategy_id,
            symbol=symbol,
            side=Side.BUY,
            quantity=quantity,
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


def sync_strategy_position_state(strategy: BaseStrategy, symbol: str, position_qty: Decimal) -> None:
    is_open = position_qty > Decimal("0")
    set_long_open = getattr(strategy, "set_long_open", None)
    if callable(set_long_open):
        set_long_open(symbol=symbol, is_open=is_open)


def run_once(
    *,
    strategy: BaseStrategy,
    adapter: Any,
    bars: pl.DataFrame,
    symbol: str,
    dry_run: bool = False,
    position_qty: Decimal | None = None,
    equity: Decimal | None = None,
    max_leverage: Decimal = Decimal("1"),
    fee_bps: Decimal = Decimal("0"),
    risk_engine: RiskEngine | None = None,
    day_pnl: Decimal = Decimal("0"),
    losing_streak: int = 0,
    symbol_notional: Decimal = Decimal("0"),
) -> dict[str, Any]:
    if "open_time" not in bars.columns:
        raise ValueError("bars must contain open_time")
    if "close" not in bars.columns:
        raise ValueError("bars must contain close")
    if not dry_run and risk_engine is None:
        raise ValueError("risk_engine is required when dry_run is False")
    if not dry_run and equity is None:
        raise ValueError("equity is required when dry_run is False")

    sorted_bars = bars.sort("open_time")
    prepared_bars = strategy.prepare_features(sorted_bars)
    replay_bars = prepared_bars if isinstance(prepared_bars, pl.DataFrame) else sorted_bars

    latest_bar = replay_bars.tail(1)
    latest_context = StrategyContext(symbol=symbol, bars=replay_bars)
    decisions = strategy.on_bar(latest_context)

    submitted_intents: list[dict[str, Any]] = []
    risk_rejections: list[dict[str, Any]] = []
    latest_open_time = int(latest_bar.get_column("open_time").item()) if latest_bar.height > 0 else None
    latest_close = Decimal(str(latest_bar.get_column("close").item())) if latest_bar.height > 0 else None

    for decision in decisions:
        if dry_run:
            continue
        intent = decision_to_intent(
            strategy.strategy_id,
            symbol,
            decision,
            position_qty=position_qty,
            latest_price=latest_close,
            equity=equity,
            max_leverage=max_leverage,
            fee_bps=fee_bps,
        )
        if intent is None:
            continue
        order_notional = Decimal("0")
        if intent.side == Side.BUY:
            if latest_close is None:
                raise ValueError("latest close is required for risk checks")
            order_notional = intent.quantity * latest_close
        risk_result = check_risk(
            risk_engine,
            RiskContext(
                symbol=symbol,
                order_notional=order_notional,
                equity=equity,
                day_pnl=day_pnl,
                losing_streak=losing_streak,
                symbol_notional=symbol_notional,
            ),
        )
        if not risk_result.allowed:
            risk_rejections.append(
                {
                    "decision_type": decision.decision_type.value,
                    "rule": risk_result.rule,
                    "detail": risk_result.detail,
                }
            )
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
        "risk_rejections": risk_rejections,
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


def run_live_loop(
    config: LiveRunConfig,
    sleep_fn=time.sleep,
    now_ms_fn=lambda: time.time_ns() // 1_000_000,
) -> int:
    if config.max_cycles is not None and config.max_cycles <= 0:
        return 0

    validate_live_mode(dry_run=config.dry_run, api_key=config.api_key, api_secret=config.api_secret)
    strategy = build_strategy(config.strategy_id)
    adapter = None
    if not config.dry_run:
        adapter = bootstrap_live_runner(api_key=config.api_key, api_secret=config.api_secret)
    risk_engine = RiskEngine(
        max_daily_loss=config.max_daily_loss,
        max_losing_streak=config.max_losing_streak,
        max_symbol_notional_ratio=config.max_symbol_notional_ratio,
    )
    state_path = Path(config.state_path)
    state = load_live_state(state_path)
    market_client = BinanceUMClient()

    cycles = 0
    started = False
    last_context: StrategyContext | None = None

    try:
        while True:
            now_ms = now_ms_fn()
            bars = fetch_closed_bars(
                client=market_client,
                symbol=config.symbol,
                interval=config.interval,
                lookback_bars=config.lookback_bars,
                now_ms=now_ms,
            )
            new_bars = select_new_closed_bars(bars, state.last_processed_open_time)

            if new_bars.height > 0:
                last_context = StrategyContext(symbol=config.symbol, bars=bars)
                if not started:
                    strategy.on_start(last_context)
                    started = True

                if config.dry_run:
                    snapshot = SimpleNamespace(
                        position_qty=Decimal("0"),
                        equity=Decimal("0"),
                        day_pnl=Decimal("0"),
                        losing_streak=0,
                        symbol_notional=Decimal("0"),
                    )
                else:
                    snapshot = adapter.fetch_account_snapshot(symbol=config.symbol, now_ms=now_ms)
                sync_strategy_position_state(strategy, config.symbol, snapshot.position_qty)
                result = run_once(
                    strategy=strategy,
                    adapter=adapter,
                    bars=bars,
                    symbol=config.symbol,
                    dry_run=config.dry_run,
                    position_qty=snapshot.position_qty,
                    equity=snapshot.equity,
                    max_leverage=config.max_leverage,
                    fee_bps=config.fee_bps,
                    risk_engine=risk_engine,
                    day_pnl=snapshot.day_pnl,
                    losing_streak=snapshot.losing_streak,
                    symbol_notional=snapshot.symbol_notional,
                )

                latest_open_time = result.get("latest_open_time")
                if latest_open_time is None:
                    latest_open_time = int(new_bars.get_column("open_time").max())
                last_submit_ts_ms = state.last_submit_ts_ms
                if int(result.get("submit_count", 0)) > 0:
                    last_submit_ts_ms = int(latest_open_time)
                state = LiveRuntimeState(
                    last_processed_open_time=int(latest_open_time),
                    last_submit_ts_ms=last_submit_ts_ms,
                )
                save_live_state(state_path, state)

            cycles += 1
            if config.max_cycles is not None and cycles >= config.max_cycles:
                break
            sleep_fn(config.poll_seconds)
    finally:
        if started and last_context is not None:
            strategy.on_finish(last_context)

    return 0


def main(argv: list[str] | None = None) -> int:
    config = LiveRunConfig.from_argv(argv)
    return run_live_loop(config)


__all__ = [
    "bootstrap_live_runner",
    "check_risk",
    "decision_to_intent",
    "main",
    "run_live_loop",
    "run_once",
    "sync_strategy_position_state",
    "validate_live_mode",
]
