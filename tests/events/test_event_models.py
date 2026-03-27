from decimal import Decimal

from cta_core.events.models import EventType, FillEvent, OrderIntent, Side


def test_fill_event_contract():
    fill = FillEvent(
        event_id="e1",
        symbol="BTCUSDT",
        side=Side.BUY,
        quantity=Decimal("0.01"),
        price=Decimal("62000"),
        fee=Decimal("0.5"),
        ts_ms=1700000000000,
    )
    assert fill.type == EventType.FILL_RECEIVED
    assert fill.notional == Decimal("620")


def test_order_intent_contract():
    intent = OrderIntent(
        strategy_id="sma_cross",
        symbol="BTCUSDT",
        side=Side.SELL,
        quantity=Decimal("0.02"),
        order_type="MARKET",
    )
    assert intent.order_type == "MARKET"
