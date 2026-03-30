from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


@dataclass(frozen=True)
class BacktestStrategyPreset:
    strategy_id: str
    description: str
    defaults: dict[str, object]

    def merged_defaults(self, overrides: Mapping[str, object] | None = None) -> dict[str, object]:
        merged = dict(self.defaults)
        if overrides:
            merged.update(overrides)
        return merged
