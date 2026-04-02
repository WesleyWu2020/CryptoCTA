from decimal import Decimal

from cta_core.app import live_runner
from cta_core.app.live_config import LiveRunConfig
from cta_core.events import Side
from cta_core.risk import RiskContext, RiskEngine
from cta_core.strategy_runtime import StrategyDecision, StrategyDecisionType


def test_decision_to_intent_maps_enter_long() -> None:
    decision = StrategyDecision(decision_type=StrategyDecisionType.ENTER_LONG, size=Decimal("0.5"))

    intent = live_runner.decision_to_intent("rp_daily_breakout", "BTCUSDT", decision)

    assert intent is not None
    assert intent.strategy_id == "rp_daily_breakout"
    assert intent.symbol == "BTCUSDT"
    assert intent.side is Side.BUY
    assert intent.quantity == Decimal("0.5")
    assert intent.order_type == "MARKET"


def test_check_risk_rejects_when_symbol_budget_exceeded() -> None:
    engine = RiskEngine(max_daily_loss=Decimal("100"))
    ctx = RiskContext(
        symbol="BTCUSDT",
        order_notional=Decimal("300"),
        equity=Decimal("1000"),
        day_pnl=Decimal("0"),
        losing_streak=0,
        symbol_notional=Decimal("150"),
    )

    result = live_runner.check_risk(engine, ctx)

    assert result.allowed is False
    assert result.rule == "symbol_risk_budget"
    assert "exceeds" in result.detail


def test_live_run_config_from_args() -> None:
    config = LiveRunConfig.from_argv(
        [
            "--strategy",
            "rp_daily_breakout",
            "--symbol",
            "BTCUSDT",
            "--interval",
            "1h",
            "--lookback-bars",
            "300",
            "--poll-seconds",
            "2",
            "--dry-run",
        ]
    )

    assert config.strategy_id == "rp_daily_breakout"
    assert config.symbol == "BTCUSDT"
    assert config.interval == "1h"
    assert config.lookback_bars == 300
    assert config.poll_seconds == 2
    assert config.dry_run is True
