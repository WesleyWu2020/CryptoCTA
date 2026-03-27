from decimal import Decimal

from cta_core.events.models import FillEvent, OrderIntent, Side


def simulate_fill(
    *,
    intent: OrderIntent,
    next_open: Decimal,
    bar_high: Decimal,
    bar_low: Decimal,
    fee_bps: Decimal,
    base_slippage_bps: Decimal,
    k: Decimal,
) -> FillEvent:
    range_bps = (bar_high - bar_low) / next_open * Decimal("10000")
    slippage_bps = base_slippage_bps + k * range_bps
    direction = Decimal("1") if intent.side == Side.BUY else Decimal("-1")
    fill_price = next_open * (Decimal("1") + direction * slippage_bps / Decimal("10000"))
    fee = intent.quantity * fill_price * fee_bps / Decimal("10000")
    return FillEvent(
        event_id="sim-fill-1",
        symbol=intent.symbol,
        side=intent.side,
        quantity=intent.quantity,
        price=fill_price,
        fee=fee,
        ts_ms=0,
    )
