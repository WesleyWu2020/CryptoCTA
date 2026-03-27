from __future__ import annotations

from hashlib import sha256


class LiveBinanceAdapter:
    BASE_URL = "https://fapi.binance.com"

    def __init__(self, api_key: str, api_secret: str) -> None:
        self.api_key = api_key
        self.api_secret = api_secret

    def client_order_id(self, *, strategy_id: str, symbol: str, ts_ms: int) -> str:
        payload = f"{strategy_id}|{symbol}|{ts_ms}".encode()
        return sha256(payload).hexdigest()[:32]
