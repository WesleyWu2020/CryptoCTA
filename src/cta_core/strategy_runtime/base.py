from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, Protocol

import polars as pl


class StrategyDecisionType(str, Enum):
    HOLD = "HOLD"
    ENTER_LONG = "ENTER_LONG"
    EXIT_LONG = "EXIT_LONG"
    ENTER_SHORT = "ENTER_SHORT"
    EXIT_SHORT = "EXIT_SHORT"


@dataclass(frozen=True)
class StrategyDecision:
    decision_type: StrategyDecisionType
    size: Decimal = Decimal("0")
    reason: str | None = None

    def __post_init__(self) -> None:
        if self.decision_type in (StrategyDecisionType.ENTER_LONG, StrategyDecisionType.ENTER_SHORT) and self.size <= 0:
            raise ValueError("size must be positive for entry decisions")


@dataclass(frozen=True)
class BacktestPosition:
    quantity: Decimal = Decimal("0")
    entry_price: Decimal | None = None

    @property
    def is_flat(self) -> bool:
        return self.quantity == 0


@dataclass(frozen=True)
class StrategyContext:
    symbol: str
    bars: pl.DataFrame

    @property
    def current_bar(self) -> dict[str, Any]:
        if self.bars.height == 0:
            raise ValueError("bars is empty")
        return self.bars.tail(1).to_dicts()[0]

    def feature_value(self, name: str, default: Any = None) -> Any:
        return self.current_bar.get(name, default)


class BaseStrategy(Protocol):
    strategy_id: str

    def prepare_features(self, bars: pl.DataFrame, bars_htf: pl.DataFrame | None = None) -> Any:
        ...

    def on_start(self, context: StrategyContext) -> None:
        ...

    def on_bar(self, context: StrategyContext) -> list[StrategyDecision]:
        ...

    def on_finish(self, context: StrategyContext) -> None:
        ...
