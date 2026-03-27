from decimal import Decimal

from cta_core.bindings.ledger import apply_fill
from cta_core.events.models import Side


def test_apply_fill_updates_position():
    state = {"position_qty": Decimal("0"), "avg_price": Decimal("0"), "realized_pnl": Decimal("0")}
    next_state = apply_fill(state, side=Side.BUY, qty=Decimal("1"), price=Decimal("100"))
    assert next_state["position_qty"] == Decimal("1")
    assert next_state["avg_price"] == Decimal("100")
