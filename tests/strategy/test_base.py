from decimal import Decimal

import polars as pl
import pytest

from cta_core.events import OrderIntent, Side
from cta_core.strategy_runtime import Strategy, StrategyContext as ExportedStrategyContext, run_bar_close
from cta_core.strategy_runtime.base import (
    BacktestPosition,
    StrategyContext,
    StrategyDecision,
    StrategyDecisionType,
)


@pytest.mark.parametrize(
    ("decision_type", "size"),
    [
        (StrategyDecisionType.ENTER_LONG, Decimal("0")),
        (StrategyDecisionType.ENTER_LONG, Decimal("-1")),
        (StrategyDecisionType.ENTER_SHORT, Decimal("0")),
        (StrategyDecisionType.ENTER_SHORT, Decimal("-1")),
    ],
)
def test_strategy_decision_rejects_non_positive_size_for_entry_decisions(decision_type, size):
    with pytest.raises(ValueError, match="size must be positive for entry decisions"):
        StrategyDecision(decision_type=decision_type, size=size)


def test_strategy_context_exposes_current_bar_and_feature_value():
    bars = pl.DataFrame({"open": [100, 101], "close": [110, 111]})
    context = StrategyContext(symbol="BTCUSDT", bars=bars)

    assert context.current_bar["open"] == 101
    assert context.current_bar["close"] == 111
    assert context.feature_value("close") == 111
    assert context.feature_value("missing", default="fallback") == "fallback"


def test_backtest_position_defaults_to_flat():
    position = BacktestPosition()

    assert position.quantity == Decimal("0")
    assert position.is_flat is True


def test_exported_strategy_supports_legacy_on_bar_close_contract():
    class LegacyStrategy(Strategy):
        strategy_id = "legacy"

        def on_bar_close(self, context: ExportedStrategyContext) -> OrderIntent | None:
            return OrderIntent(
                strategy_id=self.strategy_id,
                symbol=context.symbol,
                side=Side.BUY,
                quantity=Decimal("1"),
                order_type="MARKET",
            )

    bars = pl.DataFrame({"close": [1.0]})
    intent = run_bar_close(strategy=LegacyStrategy(), bars=bars, symbol="BTCUSDT")

    assert intent is not None
    assert intent.symbol == "BTCUSDT"
    assert intent.strategy_id == "legacy"
