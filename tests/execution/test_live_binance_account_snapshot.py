from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import hmac
from hashlib import sha256
from urllib.parse import urlencode, urlparse

from cta_core.execution import live_binance
from cta_core.execution.live_binance import LiveBinanceAdapter


@dataclass
class _FakeResponse:
    payload: object

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


def test_fetch_account_snapshot_uses_signed_endpoints(monkeypatch):
    captured: list[dict[str, object]] = []
    payloads = {
        "/fapi/v2/account": {
            "totalWalletBalance": "1000",
            "totalUnrealizedProfit": "10",
        },
        "/fapi/v2/positionRisk": [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "-0.10",
                "notional": "-2000",
            },
            {
                "symbol": "BTCUSDT",
                "positionAmt": "0.15",
                "notional": "3000",
            },
            {
                "symbol": "ETHUSDT",
                "positionAmt": "1.5",
                "notional": "4500.00",
            },
        ],
        "/fapi/v1/userTrades": [
            {"time": 1725148803000, "realizedPnl": "1"},
            {"time": 1725148802000, "realizedPnl": "-6"},
            {"time": 1725148801000, "realizedPnl": "-8"},
            {"time": 1725062400000, "realizedPnl": "-9.0"},
        ],
    }

    def fake_get(url, params, headers, timeout):
        captured.append({"url": url, "params": dict(params), "headers": headers, "timeout": timeout})
        path = urlparse(url).path
        return _FakeResponse(payloads[path])

    monkeypatch.setattr(live_binance.httpx, "get", fake_get)

    adapter = LiveBinanceAdapter(api_key="api-key", api_secret="secret")
    snapshot = adapter.fetch_account_snapshot(symbol="BTCUSDT", now_ms=1725148800000)

    assert [entry["url"] for entry in captured] == [
        "https://fapi.binance.com/fapi/v2/account",
        "https://fapi.binance.com/fapi/v2/positionRisk",
        "https://fapi.binance.com/fapi/v1/userTrades",
    ]
    for entry in captured:
        assert entry["headers"] == {"X-MBX-APIKEY": "api-key"}
        assert entry["timeout"] == 10.0
        assert "timestamp" in entry["params"]
        signable = [(key, value) for key, value in entry["params"].items() if key != "signature"]
        expected_signature = hmac.new(
            b"secret",
            urlencode(signable).encode(),
            sha256,
        ).hexdigest()
        assert entry["params"]["signature"] == expected_signature

    user_trades_request = next(entry for entry in captured if entry["url"].endswith("/fapi/v1/userTrades"))
    assert user_trades_request["params"]["startTime"] == 1725148800000
    assert user_trades_request["params"]["limit"] == 1000

    assert snapshot.equity == Decimal("1010")
    assert snapshot.symbol_notional == Decimal("5000")
    assert snapshot.position_qty == Decimal("0.25")
    assert snapshot.day_pnl == Decimal("-13")
    assert snapshot.losing_streak == 0


def test_fetch_account_snapshot_counts_losing_streak_from_latest_trades(monkeypatch):
    payloads = {
        "/fapi/v2/account": {
            "totalWalletBalance": "100",
            "totalUnrealizedProfit": "0",
        },
        "/fapi/v2/positionRisk": [
            {
                "symbol": "BTCUSDT",
                "positionAmt": "0",
                "notional": "0",
            }
        ],
        "/fapi/v1/userTrades": [
            {"time": 1725148799000, "realizedPnl": "-1"},
            {"time": 1725148798000, "realizedPnl": "-2"},
            {"time": 1725148797000, "realizedPnl": "-3"},
            {"time": 1725148796000, "realizedPnl": "4"},
            {"time": 1725148795000, "realizedPnl": "-5"},
        ],
    }

    def fake_get(url, params, headers, timeout):
        path = urlparse(url).path
        return _FakeResponse(payloads[path])

    monkeypatch.setattr(live_binance.httpx, "get", fake_get)

    adapter = LiveBinanceAdapter(api_key="api-key", api_secret="secret")
    snapshot = adapter.fetch_account_snapshot(symbol="BTCUSDT", now_ms=1725148800000)

    assert snapshot.losing_streak == 3
