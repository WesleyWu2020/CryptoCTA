from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class LiveRuntimeState:
    last_processed_open_time: int | None = None
    last_submit_ts_ms: int | None = None


def load_live_state(path: Path) -> LiveRuntimeState:
    if not path.exists():
        return LiveRuntimeState()

    data = json.loads(path.read_text())
    return LiveRuntimeState(
        last_processed_open_time=data.get("last_processed_open_time"),
        last_submit_ts_ms=data.get("last_submit_ts_ms"),
    )


def save_live_state(path: Path, state: LiveRuntimeState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "last_processed_open_time": state.last_processed_open_time,
                "last_submit_ts_ms": state.last_submit_ts_ms,
            },
            indent=2,
        )
    )


__all__ = ["LiveRuntimeState", "load_live_state", "save_live_state"]
