from decimal import Decimal

import pytest

from cta_core.events import OrderIntent, Side
from cta_core.execution.live_binance import LiveBinanceAdapter


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def test_submit_order_signs_market_order_and_uses_deterministic_client_order_id(monkeypatch):
    captured = {}

    def fake_post(url, headers, data, timeout):
        captured["url"] = url
        captured["headers"] = headers
        captured["data"] = data
        captured["timeout"] = timeout
        return _FakeResponse({"orderId": 123, "status": "NEW"})

    monkeypatch.setattr("httpx.post", fake_post)

    adapter = LiveBinanceAdapter(api_key="k", api_secret="s")
    intent = OrderIntent(
        strategy_id="sma",
        symbol="BTCUSDT",
        side=Side.BUY,
        quantity=Decimal("0.01"),
        order_type="MARKET",
    )

    result = adapter.submit_order(intent=intent, ts_ms=1725148800000)

    assert result == {"orderId": 123, "status": "NEW"}
    assert captured["url"] == "https://fapi.binance.com/fapi/v1/order"
    assert captured["headers"] == {"X-MBX-APIKEY": "k"}
    assert captured["data"]["symbol"] == "BTCUSDT"
    assert captured["data"]["side"] == "BUY"
    assert captured["data"]["type"] == "MARKET"
    assert captured["data"]["quantity"] == "0.01"
    assert captured["data"]["newClientOrderId"] == "2a8a8eeb26e06a00d244a8791e6c80cf"
    assert captured["data"]["timestamp"] == 1725148800000
    assert captured["data"]["signature"] == "568f12029fc44025ab9a21825d5357ee13972caef90f575a6fa324ecc343655b"


def test_submit_order_rejects_non_market_orders_without_http_call(monkeypatch):
    called = {"post": False}

    def fake_post(url, headers, data, timeout):
        called["post"] = True
        return _FakeResponse({"orderId": 123, "status": "NEW"})

    monkeypatch.setattr("httpx.post", fake_post)

    adapter = LiveBinanceAdapter(api_key="k", api_secret="s")
    intent = OrderIntent(
        strategy_id="sma",
        symbol="BTCUSDT",
        side=Side.BUY,
        quantity=Decimal("0.01"),
        order_type="LIMIT",
    )

    with pytest.raises(ValueError, match="MARKET orders"):
        adapter.submit_order(intent=intent, ts_ms=1725148800000)

    assert called["post"] is False
