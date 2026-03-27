from __future__ import annotations

from typing import Any

import httpx


class BinanceUMClient:
    BASE_URL = "https://fapi.binance.com"

    def fetch_klines(self, symbol: str, interval: str, limit: int = 1000) -> list[Any]:
        response = httpx.get(
            f"{self.BASE_URL}/fapi/v1/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
            timeout=10.0,
        )
        response.raise_for_status()
        return response.json()
