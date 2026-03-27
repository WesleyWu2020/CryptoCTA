from decimal import Decimal

from cta_core.events.models import OrderIntent, Side
from cta_core.execution.sim_engine import simulate_fill


def test_simulate_fill_applies_directional_slippage_and_fee():
    intent = OrderIntent("sma_cross", "BTCUSDT", Side.BUY, Decimal("0.01"), "MARKET")
    fill = simulate_fill(
        intent=intent,
        next_open=Decimal("60000"),
        bar_high=Decimal("60600"),
        bar_low=Decimal("59400"),
        fee_bps=Decimal("4"),
        base_slippage_bps=Decimal("1"),
        k=Decimal("0.2"),
    )
    assert fill.price > Decimal("60000")
    assert fill.fee > Decimal("0")
