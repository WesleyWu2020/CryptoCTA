from decimal import Decimal

import polars as pl

from cta_core.bindings.ledger import apply_fill
from cta_core.risk.engine import RiskContext, RiskEngine
from cta_core.execution.sim_engine import simulate_fill
from cta_core.strategy_runtime.runtime import run_bar_close
from cta_core.strategy_runtime.strategies.sma_cross import SmaCrossStrategy


def test_backtest_pipeline_strategy_risk_fill_ledger():
    bars = pl.DataFrame(
        {
            "open_time": [1, 2, 3, 4, 5, 6],
            "open": [100, 101, 102, 103, 104, 105],
            "high": [101, 102, 103, 104, 106, 107],
            "low": [99, 100, 101, 102, 103, 104],
            "close": [100, 101, 102, 103, 105, 106],
        }
    )

    strategy = SmaCrossStrategy(fast=2, slow=3)
    intent = run_bar_close(strategy=strategy, bars=bars, symbol="BTCUSDT")
    assert intent is not None

    risk_engine = RiskEngine(max_daily_loss=Decimal("1000"))
    risk_ctx = RiskContext(
        symbol="BTCUSDT",
        order_notional=intent.quantity * Decimal(str(bars["close"][-1])),
        equity=Decimal("10000"),
        day_pnl=Decimal("0"),
        losing_streak=0,
        symbol_notional=Decimal("0"),
    )
    risk_result = risk_engine.check(risk_ctx)
    assert risk_result.allowed is True

    fill = simulate_fill(
        intent=intent,
        next_open=Decimal(str(bars["open"][-1])),
        bar_high=Decimal(str(bars["high"][-1])),
        bar_low=Decimal(str(bars["low"][-1])),
        fee_bps=Decimal("4"),
        base_slippage_bps=Decimal("1"),
        k=Decimal("0.2"),
    )
    assert fill.fee > Decimal("0")

    state = {"position_qty": Decimal("0"), "avg_price": Decimal("0"), "realized_pnl": Decimal("0")}
    next_state = apply_fill(state, side=fill.side, qty=fill.quantity, price=fill.price)
    assert next_state["position_qty"] == fill.quantity
    assert next_state["avg_price"] == fill.price


def test_backtest_pipeline_stops_on_risk_rejection():
    bars = pl.DataFrame(
        {
            "open_time": [1, 2, 3, 4, 5, 6],
            "open": [100, 101, 102, 103, 104, 105],
            "high": [101, 102, 103, 104, 106, 107],
            "low": [99, 100, 101, 102, 103, 104],
            "close": [100, 101, 102, 103, 105, 106],
        }
    )

    strategy = SmaCrossStrategy(fast=2, slow=3)
    intent = run_bar_close(strategy=strategy, bars=bars, symbol="BTCUSDT")
    assert intent is not None

    risk_engine = RiskEngine(max_daily_loss=Decimal("100"))
    risk_ctx = RiskContext(
        symbol="BTCUSDT",
        order_notional=intent.quantity * Decimal(str(bars["close"][-1])),
        equity=Decimal("10000"),
        day_pnl=Decimal("-150"),
        losing_streak=0,
        symbol_notional=Decimal("0"),
    )
    risk_result = risk_engine.check(risk_ctx)
    assert risk_result.allowed is False
    assert risk_result.rule == "daily_max_loss"
