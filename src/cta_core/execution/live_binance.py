from __future__ import annotations

import hmac
from hashlib import sha256
from urllib.parse import urlencode

import httpx

from cta_core.events import OrderIntent


class LiveBinanceAdapter:
    BASE_URL = "https://fapi.binance.com"

    def __init__(self, api_key: str, api_secret: str) -> None:
        self.api_key = api_key
        self.api_secret = api_secret

    def client_order_id(self, *, strategy_id: str, symbol: str, ts_ms: int) -> str:
        payload = f"{strategy_id}|{symbol}|{ts_ms}".encode()
        return sha256(payload).hexdigest()[:32]

    def submit_order(self, *, intent: OrderIntent, ts_ms: int) -> dict:
        if intent.order_type.upper() != "MARKET":
            raise ValueError("LiveBinanceAdapter.submit_order only supports MARKET orders")

        payload = {
            "symbol": intent.symbol,
            "side": intent.side.value,
            "type": intent.order_type.upper(),
            "quantity": str(intent.quantity),
            "newClientOrderId": self.client_order_id(
                strategy_id=intent.strategy_id,
                symbol=intent.symbol,
                ts_ms=ts_ms,
            ),
            "timestamp": ts_ms,
            "recvWindow": 5000,
        }
        query = urlencode(payload)
        payload["signature"] = hmac.new(
            self.api_secret.encode(),
            query.encode(),
            sha256,
        ).hexdigest()

        response = httpx.post(
            f"{self.BASE_URL}/fapi/v1/order",
            headers={"X-MBX-APIKEY": self.api_key},
            data=payload,
            timeout=10.0,
        )
        response.raise_for_status()
        return response.json()
