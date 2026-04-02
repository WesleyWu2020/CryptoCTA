from __future__ import annotations

import hmac
import time
from decimal import Decimal
from hashlib import sha256
from urllib.parse import urlencode

import httpx

from cta_core.events.models import OrderIntent


class LiveBinanceAdapter:
    BASE_URL = "https://fapi.binance.com"

    def __init__(self, api_key: str, api_secret: str) -> None:
        self.api_key = api_key
        self.api_secret = api_secret

    def client_order_id(self, *, strategy_id: str, symbol: str, ts_ms: int) -> str:
        payload = f"{strategy_id}|{symbol}|{ts_ms}".encode()
        return sha256(payload).hexdigest()[:32]

    def _sign_query_params(self, params: dict[str, object]) -> str:
        query_string = urlencode(params)
        return hmac.new(
            self.api_secret.encode(),
            query_string.encode(),
            sha256,
        ).hexdigest()

    @staticmethod
    def _format_quantity(quantity: Decimal) -> str:
        formatted = format(quantity, "f").rstrip("0").rstrip(".")
        return formatted if formatted else "0"

    def submit_order(self, intent: OrderIntent, ts_ms: int | None = None) -> dict[str, object]:
        if intent.order_type != "MARKET":
            raise ValueError(f"Unsupported order_type {intent.order_type!r}; MARKET-only is supported for now")

        timestamp = ts_ms if ts_ms is not None else time.time_ns() // 1_000_000
        new_client_order_id = self.client_order_id(
            strategy_id=intent.strategy_id,
            symbol=intent.symbol,
            ts_ms=timestamp,
        )
        params: dict[str, object] = {
            "symbol": intent.symbol,
            "side": intent.side.value,
            "type": intent.order_type,
            "quantity": self._format_quantity(intent.quantity),
            "newClientOrderId": new_client_order_id,
            "timestamp": timestamp,
        }
        params["signature"] = self._sign_query_params(params)

        response = httpx.post(
            f"{self.BASE_URL}/fapi/v1/order",
            params=params,
            headers={"X-MBX-APIKEY": self.api_key},
            timeout=10.0,
        )
        response.raise_for_status()
        return response.json()
