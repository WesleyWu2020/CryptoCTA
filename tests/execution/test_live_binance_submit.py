from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256
import hmac
from urllib.parse import urlencode

import httpx
import pytest

from cta_core.events.models import OrderIntent, Side
from cta_core.execution import live_binance
from cta_core.execution.live_binance import LiveBinanceAdapter


@dataclass
class _FakeResponse:
    payload: dict[str, object]

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def test_submit_order_sends_signed_request_and_returns_json(monkeypatch):
    captured = {}

    def fake_post(url, params, headers, timeout):
        captured["url"] = url
        captured["params"] = params
        captured["headers"] = headers
        captured["timeout"] = timeout
        return _FakeResponse({"orderId": 123, "status": "NEW"})

    monkeypatch.setattr(live_binance.httpx, "post", fake_post)

    adapter = LiveBinanceAdapter(api_key="api-key", api_secret="secret")
    intent = OrderIntent(
        strategy_id="sma_cross",
        symbol="BTCUSDT",
        side=Side.BUY,
        quantity=Decimal("0.01"),
        order_type="MARKET",
    )

    result = adapter.submit_order(intent, ts_ms=1725148800000)

    expected_client_order_id = adapter.client_order_id(
        strategy_id="sma_cross",
        symbol="BTCUSDT",
        side=Side.BUY,
        order_type="MARKET",
        quantity=Decimal("0.01"),
        ts_ms=1725148800000,
    )
    signable_params = [
        ("symbol", "BTCUSDT"),
        ("side", "BUY"),
        ("type", "MARKET"),
        ("quantity", "0.01"),
        ("newClientOrderId", expected_client_order_id),
        ("timestamp", 1725148800000),
    ]
    expected_signature = hmac.new(
        b"secret",
        urlencode(signable_params).encode(),
        sha256,
    ).hexdigest()

    assert result == {"orderId": 123, "status": "NEW"}
    assert captured["url"] == "https://fapi.binance.com/fapi/v1/order"
    assert captured["headers"] == {"X-MBX-APIKEY": "api-key"}
    assert captured["params"] == {
        "symbol": "BTCUSDT",
        "side": "BUY",
        "type": "MARKET",
        "quantity": "0.01",
        "newClientOrderId": expected_client_order_id,
        "timestamp": 1725148800000,
        "signature": expected_signature,
    }


def test_submit_order_rejects_unsupported_order_type():
    adapter = LiveBinanceAdapter(api_key="api-key", api_secret="secret")
    intent = OrderIntent(
        strategy_id="sma_cross",
        symbol="BTCUSDT",
        side=Side.BUY,
        quantity=Decimal("0.01"),
        order_type="LIMIT",
        limit_price=Decimal("100"),
    )

    with pytest.raises(ValueError, match="MARKET-only"):
        adapter.submit_order(intent, ts_ms=1725148800000)


def test_submit_order_formats_tiny_quantity_without_scientific_notation(monkeypatch):
    captured = {}

    def fake_post(url, params, headers, timeout):
        captured["params"] = params
        return _FakeResponse({"orderId": 456})

    monkeypatch.setattr(live_binance.httpx, "post", fake_post)

    adapter = LiveBinanceAdapter(api_key="api-key", api_secret="secret")
    intent = OrderIntent(
        strategy_id="sma_cross",
        symbol="BTCUSDT",
        side=Side.BUY,
        quantity=Decimal("0.00000001"),
        order_type="MARKET",
    )

    adapter.submit_order(intent, ts_ms=1725148800000)

    assert captured["params"]["quantity"] == "0.00000001"


def test_submit_order_uses_client_tag_to_reduce_same_bar_collisions(monkeypatch):
    captured = {}

    def fake_post(url, params, headers, timeout):
        captured["params"] = params
        return _FakeResponse({"orderId": 789, "status": "NEW"})

    monkeypatch.setattr(live_binance.httpx, "post", fake_post)

    adapter = LiveBinanceAdapter(api_key="api-key", api_secret="secret")
    intent = OrderIntent(
        strategy_id="sma_cross",
        symbol="BTCUSDT",
        side=Side.BUY,
        quantity=Decimal("0.01"),
        order_type="MARKET",
    )

    result = adapter.submit_order(intent, ts_ms=1725148800000, client_tag="1")

    expected_client_order_id = adapter.client_order_id(
        strategy_id="sma_cross",
        symbol="BTCUSDT",
        side=Side.BUY,
        order_type="MARKET",
        quantity=Decimal("0.01"),
        ts_ms=1725148800000,
        client_tag="1",
    )

    assert result == {"orderId": 789, "status": "NEW"}
    assert captured["params"]["newClientOrderId"] == expected_client_order_id


def test_submit_order_returns_idempotent_duplicate_marker_for_duplicate_client_order_id(monkeypatch):
    adapter = LiveBinanceAdapter(api_key="api-key", api_secret="secret")
    intent = OrderIntent(
        strategy_id="sma_cross",
        symbol="BTCUSDT",
        side=Side.BUY,
        quantity=Decimal("0.01"),
        order_type="MARKET",
    )

    def fake_post(url, params, headers, timeout):
        request = httpx.Request("POST", url, params=params, headers=headers)
        return httpx.Response(
            400,
            request=request,
            json={"code": -4111, "msg": "Client order ID is not unique."},
        )

    monkeypatch.setattr(live_binance.httpx, "post", fake_post)

    result = adapter.submit_order(intent, ts_ms=1725148800000, client_tag="1")
    expected_client_order_id = adapter.client_order_id(
        strategy_id="sma_cross",
        symbol="BTCUSDT",
        side=Side.BUY,
        order_type="MARKET",
        quantity=Decimal("0.01"),
        ts_ms=1725148800000,
        client_tag="1",
    )

    assert result == {
        "status": "DUPLICATE_CLIENT_ORDER_ID",
        "duplicate": True,
        "clientOrderId": expected_client_order_id,
        "error_code": -4111,
        "error_message": "Client order ID is not unique.",
    }


def test_client_order_id_differs_for_distinct_intents_at_same_timestamp():
    adapter = LiveBinanceAdapter(api_key="api-key", api_secret="secret")

    enter_id = adapter.client_order_id(
        strategy_id="sma_cross",
        symbol="BTCUSDT",
        side=Side.BUY,
        order_type="MARKET",
        quantity=Decimal("0.01"),
        ts_ms=1725148800000,
    )
    exit_id = adapter.client_order_id(
        strategy_id="sma_cross",
        symbol="BTCUSDT",
        side=Side.SELL,
        order_type="MARKET",
        quantity=Decimal("0.02"),
        ts_ms=1725148800000,
    )

    assert enter_id != exit_id


def test_client_order_id_differs_for_distinct_client_tags_at_same_timestamp():
    adapter = LiveBinanceAdapter(api_key="api-key", api_secret="secret")

    first_id = adapter.client_order_id(
        strategy_id="sma_cross",
        symbol="BTCUSDT",
        side=Side.BUY,
        order_type="MARKET",
        quantity=Decimal("0.01"),
        ts_ms=1725148800000,
        client_tag="0",
    )
    second_id = adapter.client_order_id(
        strategy_id="sma_cross",
        symbol="BTCUSDT",
        side=Side.BUY,
        order_type="MARKET",
        quantity=Decimal("0.01"),
        ts_ms=1725148800000,
        client_tag="1",
    )

    assert first_id != second_id
