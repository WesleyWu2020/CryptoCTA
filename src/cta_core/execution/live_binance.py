from __future__ import annotations

import hmac
import time
from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256
from datetime import datetime, timezone
from urllib.parse import urlencode

import httpx

from cta_core.events.models import OrderIntent


@dataclass(frozen=True)
class LiveAccountSnapshot:
    equity: Decimal
    day_pnl: Decimal
    losing_streak: int
    symbol_notional: Decimal
    position_qty: Decimal


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

    def _signed_get(self, path: str, params: dict[str, object] | None = None) -> object:
        request_params: dict[str, object] = dict(params or {})
        if "timestamp" not in request_params:
            request_params["timestamp"] = time.time_ns() // 1_000_000
        request_params["signature"] = self._sign_query_params(request_params)
        response = httpx.get(
            f"{self.BASE_URL}{path}",
            params=request_params,
            headers={"X-MBX-APIKEY": self.api_key},
            timeout=10.0,
        )
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _utc_day_start_ms(now_ms: int) -> int:
        dt = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
        start = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
        return int(start.timestamp() * 1000)

    @staticmethod
    def _count_losing_streak(trades: list[dict[str, object]]) -> int:
        streak = 0
        for trade in sorted(trades, key=lambda trade: int(trade["time"]), reverse=True):
            realized_pnl = Decimal(str(trade["realizedPnl"]))
            if realized_pnl < 0:
                streak += 1
                continue
            break
        return streak

    @staticmethod
    def _sum_matching_positions(positions: list[dict[str, object]], symbol: str) -> tuple[Decimal, Decimal]:
        symbol_notional = Decimal("0")
        position_qty = Decimal("0")
        for position in positions:
            if position.get("symbol") != symbol:
                continue
            symbol_notional += abs(Decimal(str(position["notional"])))
            position_qty += Decimal(str(position["positionAmt"]))
        return symbol_notional, position_qty

    def fetch_account_snapshot(self, symbol: str, now_ms: int | None = None) -> LiveAccountSnapshot:
        current_ms = now_ms if now_ms is not None else time.time_ns() // 1_000_000
        day_start_ms = self._utc_day_start_ms(current_ms)
        account = self._signed_get("/fapi/v2/account")
        positions = self._signed_get("/fapi/v2/positionRisk")
        trades = self._signed_get(
            "/fapi/v1/userTrades",
            params={"symbol": symbol, "limit": 1000, "startTime": day_start_ms},
        )

        equity = Decimal(str(account["totalWalletBalance"])) + Decimal(str(account["totalUnrealizedProfit"]))

        symbol_notional, position_qty = self._sum_matching_positions(positions, symbol)
        day_pnl = sum(
            (
                Decimal(str(trade["realizedPnl"]))
                for trade in trades
                if int(trade["time"]) >= day_start_ms
            ),
            start=Decimal("0"),
        )

        return LiveAccountSnapshot(
            equity=equity,
            day_pnl=day_pnl,
            losing_streak=self._count_losing_streak(trades),
            symbol_notional=symbol_notional,
            position_qty=position_qty,
        )

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
