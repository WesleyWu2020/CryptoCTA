from cta_core.data.binance_client import BinanceUMClient


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def test_fetch_klines_supports_start_and_end_time(monkeypatch):
    captured = {}

    def fake_get(url, params, timeout):
        captured["url"] = url
        captured["params"] = params
        captured["timeout"] = timeout
        return _FakeResponse([[1, "100", "101", "99", "100", "10", 2, "0", 0, "0", "0", "0"]])

    monkeypatch.setattr("cta_core.data.binance_client.httpx.get", fake_get)

    client = BinanceUMClient()
    rows = client.fetch_klines(
        symbol="BTCUSDT",
        interval="15m",
        limit=1000,
        start_time=1725148800000,
        end_time=1725235200000,
    )

    assert len(rows) == 1
    assert captured["url"].endswith("/fapi/v1/klines")
    assert captured["params"] == {
        "symbol": "BTCUSDT",
        "interval": "15m",
        "limit": 1000,
        "startTime": 1725148800000,
        "endTime": 1725235200000,
    }


def test_fetch_klines_without_optional_times(monkeypatch):
    captured = {}

    def fake_get(url, params, timeout):
        captured["params"] = params
        return _FakeResponse([])

    monkeypatch.setattr("cta_core.data.binance_client.httpx.get", fake_get)

    client = BinanceUMClient()
    client.fetch_klines(symbol="BTCUSDT", interval="15m", limit=500)

    assert captured["params"] == {"symbol": "BTCUSDT", "interval": "15m", "limit": 500}
