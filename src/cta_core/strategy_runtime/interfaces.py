from __future__ import annotations

from abc import ABC, abstractmethod

from cta_core.events import OrderIntent

from .base import StrategyContext


class Strategy(ABC):
    strategy_id: str

    @abstractmethod
    def on_bar_close(self, context: StrategyContext) -> OrderIntent | None:
        raise NotImplementedError

__all__ = ["Strategy", "StrategyContext"]
