from __future__ import annotations

from dataclasses import dataclass
from abc import ABC, abstractmethod

import polars as pl

from cta_core.events import OrderIntent


@dataclass(frozen=True)
class StrategyContext:
    symbol: str
    bars: pl.DataFrame


class Strategy(ABC):
    strategy_id: str

    @abstractmethod
    def on_bar_close(self, context: StrategyContext) -> OrderIntent | None:
        raise NotImplementedError
