from decimal import Decimal

from cta_core.risk.engine import RiskContext, RiskEngine


def test_risk_engine_rejects_daily_loss_breach():
    engine = RiskEngine(max_daily_loss=Decimal("500"))
    ctx = RiskContext(
        symbol="BTCUSDT",
        order_notional=Decimal("1000"),
        equity=Decimal("10000"),
        day_pnl=Decimal("-650"),
        losing_streak=1,
        symbol_notional=Decimal("2000"),
    )
    result = engine.check(ctx)
    assert result.allowed is False
    assert result.rule == "daily_max_loss"


def test_risk_engine_rejects_losing_streak_breach():
    engine = RiskEngine(max_daily_loss=Decimal("500"), max_losing_streak=3)
    ctx = RiskContext(
        symbol="BTCUSDT",
        order_notional=Decimal("1000"),
        equity=Decimal("10000"),
        day_pnl=Decimal("-100"),
        losing_streak=3,
        symbol_notional=Decimal("2000"),
    )
    result = engine.check(ctx)
    assert result.allowed is False
    assert result.rule == "losing_streak"


def test_risk_engine_rejects_symbol_budget_breach():
    engine = RiskEngine(max_daily_loss=Decimal("500"), max_symbol_notional_ratio=Decimal("0.4"))
    ctx = RiskContext(
        symbol="BTCUSDT",
        order_notional=Decimal("1000"),
        equity=Decimal("10000"),
        day_pnl=Decimal("-100"),
        losing_streak=1,
        symbol_notional=Decimal("5000"),
    )
    result = engine.check(ctx)
    assert result.allowed is False
    assert result.rule == "symbol_risk_budget"


def test_risk_engine_allows_pass_case():
    engine = RiskEngine(max_daily_loss=Decimal("500"))
    ctx = RiskContext(
        symbol="BTCUSDT",
        order_notional=Decimal("1000"),
        equity=Decimal("10000"),
        day_pnl=Decimal("-100"),
        losing_streak=1,
        symbol_notional=Decimal("2000"),
    )
    result = engine.check(ctx)
    assert result.allowed is True
    assert result.rule == "pass"
    assert result.detail == "ok"
