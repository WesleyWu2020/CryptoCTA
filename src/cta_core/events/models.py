from dataclasses import dataclass
from decimal import Decimal
from enum import Enum


class EventType(str, Enum):
    BAR_CLOSED = "BAR_CLOSED"
    SIGNAL_GENERATED = "SIGNAL_GENERATED"
    RISK_CHECKED = "RISK_CHECKED"
    ORDER_SUBMITTED = "ORDER_SUBMITTED"
    FILL_RECEIVED = "FILL_RECEIVED"
    PORTFOLIO_UPDATED = "PORTFOLIO_UPDATED"


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass(frozen=True)
class OrderIntent:
    strategy_id: str
    symbol: str
    side: Side
    quantity: Decimal
    order_type: str
    limit_price: Decimal | None = None


@dataclass(frozen=True)
class FillEvent:
    event_id: str
    symbol: str
    side: Side
    quantity: Decimal
    price: Decimal
    fee: Decimal
    ts_ms: int
    type: EventType = EventType.FILL_RECEIVED

    @property
    def notional(self) -> Decimal:
        return self.quantity * self.price
