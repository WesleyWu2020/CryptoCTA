from decimal import Decimal

import cta_ledger

from cta_core.events.models import Side


def apply_fill(state: dict, side: Side, qty: Decimal, price: Decimal) -> dict:
    next_qty, next_avg = cta_ledger.apply_fill_py(
        float(state["position_qty"]),
        float(state["avg_price"]),
        side.value,
        float(qty),
        float(price),
    )
    return {
        "position_qty": Decimal(str(next_qty)),
        "avg_price": Decimal(str(next_avg)),
        "realized_pnl": state["realized_pnl"],
    }
