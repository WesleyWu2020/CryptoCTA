from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class LiveRuntimeState:
    last_processed_open_time: int | None = None
    last_submit_ts_ms: int | None = None


def load_live_state(path: Path) -> LiveRuntimeState:
    if not path.exists():
        return LiveRuntimeState()

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return LiveRuntimeState()

    if not isinstance(data, dict):
        return LiveRuntimeState()

    return LiveRuntimeState(
        last_processed_open_time=_coerce_optional_int(
            data.get("last_processed_open_time")
        ),
        last_submit_ts_ms=_coerce_optional_int(data.get("last_submit_ts_ms")),
    )


def save_live_state(path: Path, state: LiveRuntimeState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(
        {
            "last_processed_open_time": state.last_processed_open_time,
            "last_submit_ts_ms": state.last_submit_ts_ms,
        },
        indent=2,
    )
    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            delete=False,
        ) as tmp_file:
            tmp_path = Path(tmp_file.name)
            tmp_file.write(payload)
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
        os.replace(tmp_path, path)
    finally:
        if tmp_path is not None and tmp_path.exists():
            tmp_path.unlink()


def _coerce_optional_int(value: object) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else None
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


__all__ = ["LiveRuntimeState", "load_live_state", "save_live_state"]
