from __future__ import annotations

from typing import Any

import httpx


class BinanceUMClient:
    BASE_URL = "https://fapi.binance.com"

    def fetch_klines(
        self,
        symbol: str,
        interval: str,
        limit: int = 1000,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> list[Any]:
        params: dict[str, int | str] = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        }
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time

        response = httpx.get(
            f"{self.BASE_URL}/fapi/v1/klines",
            params=params,
            timeout=10.0,
        )
        response.raise_for_status()
        return response.json()
