from decimal import Decimal

from cta_core.events import Side
from cta_core.execution.live_binance import LiveBinanceAdapter


def test_idempotency_key_is_stable():
    adapter = LiveBinanceAdapter(api_key="k", api_secret="s")
    key1 = adapter.client_order_id(strategy_id="sma", symbol="BTCUSDT", ts_ms=1000)
    key2 = adapter.client_order_id(strategy_id="sma", symbol="BTCUSDT", ts_ms=1000)
    enriched = adapter.client_order_id(
        strategy_id="sma",
        symbol="BTCUSDT",
        side=Side.BUY,
        order_type="MARKET",
        quantity=Decimal("0.01"),
        ts_ms=1000,
    )

    assert key1 == key2
    assert enriched != key1
