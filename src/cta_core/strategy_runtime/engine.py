from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import polars as pl

from cta_core.strategy_runtime.base import BacktestPosition, BaseStrategy, StrategyContext, StrategyDecision, StrategyDecisionType


def _to_decimal(value: Any) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if value is None:
        return Decimal("0")
    return Decimal(str(value))


@dataclass
class BacktestEngine:
    symbol: str
    interval: str
    initial_equity: Decimal
    fee_bps: int
    slippage_bps: int

    def run(self, *, strategy: BaseStrategy, bars: pl.DataFrame, bars_htf: pl.DataFrame | None = None) -> dict[str, Any]:
        prepared_bars = strategy.prepare_features(bars, bars_htf)
        replay_bars = prepared_bars if isinstance(prepared_bars, pl.DataFrame) else bars

        trades: list[dict[str, Any]] = []
        position = BacktestPosition()
        equity = _to_decimal(self.initial_equity)
        entry_fee_rate = Decimal(self.fee_bps) / Decimal("10000")
        slippage_rate = Decimal(self.slippage_bps) / Decimal("10000")

        start_context = StrategyContext(symbol=self.symbol, bars=replay_bars)
        strategy.on_start(start_context)

        for index in range(replay_bars.height):
            bar_slice = replay_bars.head(index + 1)
            context = StrategyContext(symbol=self.symbol, bars=bar_slice)
            for decision in strategy.on_bar(context):
                if decision.decision_type == StrategyDecisionType.ENTER_LONG and position.is_flat:
                    price = self._execution_price(context.current_bar, side="BUY", slippage_rate=slippage_rate)
                    fee = price * _to_decimal(decision.size) * entry_fee_rate
                    position = BacktestPosition(quantity=_to_decimal(decision.size), entry_price=price)
                    equity -= price * position.quantity + fee
                    trades.append(self._trade_record(context, decision, price))
                elif decision.decision_type == StrategyDecisionType.EXIT_LONG and not position.is_flat:
                    price = self._execution_price(context.current_bar, side="SELL", slippage_rate=slippage_rate)
                    fee = price * position.quantity * entry_fee_rate
                    equity += price * position.quantity - fee
                    closed_quantity = position.quantity
                    position = BacktestPosition()
                    trades.append(self._trade_record(context, decision, price, quantity=closed_quantity))

        finish_context = StrategyContext(symbol=self.symbol, bars=replay_bars)
        strategy.on_finish(finish_context)

        return {"trades": trades, "summary": {"trade_count": len(trades)}}

    def _trade_record(self, context: StrategyContext, decision: StrategyDecision, price: Decimal, *, quantity: Decimal | None = None) -> dict[str, Any]:
        return {
            "timestamp": self._bar_timestamp(context.current_bar),
            "action": decision.decision_type.value,
            "price": price,
            "reason": decision.reason,
            "quantity": _to_decimal(quantity if quantity is not None else decision.size),
        }

    def _execution_price(self, bar: dict[str, Any], *, side: str, slippage_rate: Decimal) -> Decimal:
        base_price = self._bar_price(bar)
        if side == "BUY":
            return base_price * (Decimal("1") + slippage_rate)
        return base_price * (Decimal("1") - slippage_rate)

    @staticmethod
    def _bar_price(bar: dict[str, Any]) -> Decimal:
        for key in ("close", "price"):
            value = bar.get(key)
            if value is not None:
                base_price = _to_decimal(value)
                if not base_price.is_finite() or base_price <= 0:
                    raise ValueError("bar must contain positive close or price")
                return base_price
        raise ValueError("bar must contain positive close or price")

    @staticmethod
    def _bar_timestamp(bar: dict[str, Any]) -> Any:
        for key in ("ts_ms", "timestamp", "ts"):
            if key in bar:
                return bar[key]
        return None
