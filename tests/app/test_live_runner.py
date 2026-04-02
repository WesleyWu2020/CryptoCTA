from decimal import Decimal

import pytest
import polars as pl

from cta_core.app import live_runner
from cta_core.app.live_config import LiveRunConfig
from cta_core.events import Side
from cta_core.risk import RiskContext, RiskEngine
from cta_core.strategy_runtime import StrategyDecision, StrategyDecisionType
from cta_core.strategy_runtime.strategies import RPDailyBreakoutConfig, RPDailyBreakoutStrategy


class DummyAdapter:
    def __init__(self) -> None:
        self.submitted: list[dict[str, object]] = []

    def submit_order(self, *, intent, ts_ms: int) -> dict[str, object]:
        record = {"intent": intent, "ts_ms": ts_ms}
        self.submitted.append(record)
        return record


class HistoryAwareStrategy:
    strategy_id = "history_aware"

    def __init__(self) -> None:
        self.observed_heights: list[int] = []

    def prepare_features(self, bars: pl.DataFrame, bars_htf: pl.DataFrame | None = None) -> pl.DataFrame:
        return bars

    def on_start(self, context) -> None:
        return None

    def on_bar(self, context) -> list[StrategyDecision]:
        self.observed_heights.append(context.bars.height)
        if context.bars.height >= 2:
            return [
                StrategyDecision(
                    decision_type=StrategyDecisionType.ENTER_LONG,
                    size=Decimal("1"),
                    reason="history_seen",
                )
            ]
        return []

    def on_finish(self, context) -> None:
        return None


class EnterLongStrategy:
    strategy_id = "enter_long"

    def prepare_features(self, bars: pl.DataFrame, bars_htf: pl.DataFrame | None = None) -> pl.DataFrame:
        return bars

    def on_start(self, context) -> None:
        return None

    def on_bar(self, context) -> list[StrategyDecision]:
        return [
            StrategyDecision(
                decision_type=StrategyDecisionType.ENTER_LONG,
                size=Decimal("1"),
                reason="enter",
            )
        ]

    def on_finish(self, context) -> None:
        return None


def test_decision_to_intent_maps_enter_long() -> None:
    decision = StrategyDecision(decision_type=StrategyDecisionType.ENTER_LONG, size=Decimal("0.5"))

    intent = live_runner.decision_to_intent("rp_daily_breakout", "BTCUSDT", decision)

    assert intent is not None
    assert intent.strategy_id == "rp_daily_breakout"
    assert intent.symbol == "BTCUSDT"
    assert intent.side is Side.BUY
    assert intent.quantity == Decimal("0.5")
    assert intent.order_type == "MARKET"


def test_decision_to_intent_maps_exit_long_with_explicit_size() -> None:
    decision = StrategyDecision(decision_type=StrategyDecisionType.EXIT_LONG, size=Decimal("0.25"))

    intent = live_runner.decision_to_intent("rp_daily_breakout", "BTCUSDT", decision)

    assert intent is not None
    assert intent.strategy_id == "rp_daily_breakout"
    assert intent.symbol == "BTCUSDT"
    assert intent.side is Side.SELL
    assert intent.quantity == Decimal("0.25")
    assert intent.order_type == "MARKET"


def test_decision_to_intent_raises_for_exit_long_without_quantity_context() -> None:
    decision = StrategyDecision(decision_type=StrategyDecisionType.EXIT_LONG, size=Decimal("0"))

    with pytest.raises(ValueError, match="cannot map EXIT_LONG without positive size/position qty"):
        live_runner.decision_to_intent("rp_daily_breakout", "BTCUSDT", decision)


def test_decision_to_intent_maps_exit_long_with_position_qty_fallback() -> None:
    decision = StrategyDecision(decision_type=StrategyDecisionType.EXIT_LONG, size=Decimal("0"))

    intent = live_runner.decision_to_intent(
        "rp_daily_breakout",
        "BTCUSDT",
        decision,
        position_qty=Decimal("1.5"),
    )

    assert intent is not None
    assert intent.strategy_id == "rp_daily_breakout"
    assert intent.symbol == "BTCUSDT"
    assert intent.side is Side.SELL
    assert intent.quantity == Decimal("1.5")
    assert intent.order_type == "MARKET"


def test_decision_to_intent_returns_none_for_unsupported_decision_type() -> None:
    decision = StrategyDecision(decision_type=StrategyDecisionType.HOLD)

    intent = live_runner.decision_to_intent("rp_daily_breakout", "BTCUSDT", decision)

    assert intent is None


def test_run_once_dry_run_does_not_submit() -> None:
    bars = pl.DataFrame(
        {
            "open_time": [3_000, 1_000, 2_000],
            "close": [13.0, 10.0, 12.0],
        }
    )
    strategy = RPDailyBreakoutStrategy(
        RPDailyBreakoutConfig(rp_window=2, entry_confirmations=2, exit_confirmations=2, quantity=Decimal("1"))
    )
    adapter = DummyAdapter()

    result = live_runner.run_once(
        strategy=strategy,
        adapter=adapter,
        bars=bars,
        symbol="BTCUSDT",
        dry_run=True,
    )

    assert adapter.submitted == []
    assert result["decisions_count"] == 1
    assert result["submit_count"] == 0
    assert result["submitted_intents"] == []
    assert result["latest_open_time"] == 3_000
    assert result["dry_run"] is True


def test_run_once_uses_full_history_context() -> None:
    bars = pl.DataFrame(
        {
            "open_time": [2_000, 1_000],
            "close": [12.0, 10.0],
        }
    )
    strategy = HistoryAwareStrategy()
    adapter = DummyAdapter()

    result = live_runner.run_once(
        strategy=strategy,
        adapter=adapter,
        bars=bars,
        symbol="BTCUSDT",
        dry_run=True,
    )

    assert strategy.observed_heights == [2]
    assert result["decisions_count"] == 1
    assert result["submit_count"] == 0
    assert adapter.submitted == []


def test_run_once_non_dry_submit_uses_latest_open_time() -> None:
    bars = pl.DataFrame(
        {
            "open_time": [2_000, 3_000, 1_000],
            "close": [12.0, 13.0, 10.0],
        }
    )
    strategy = EnterLongStrategy()
    adapter = DummyAdapter()

    result = live_runner.run_once(
        strategy=strategy,
        adapter=adapter,
        bars=bars,
        symbol="BTCUSDT",
        dry_run=False,
    )

    assert result["decisions_count"] == 1
    assert result["submit_count"] == 1
    assert len(adapter.submitted) == 1
    assert adapter.submitted[0]["ts_ms"] == 3_000


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
