from __future__ import annotations

import json
from pathlib import Path

import pytest

from cta_core.app.live_state import (
    LiveRuntimeState,
    load_live_state,
    save_live_state,
)


def test_load_live_state_returns_defaults_when_file_missing(tmp_path: Path) -> None:
    state = load_live_state(tmp_path / "live_state.json")

    assert state == LiveRuntimeState()


def test_save_live_state_then_load_live_state_round_trips_values(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "live_state.json"
    state = LiveRuntimeState(
        last_processed_open_time=1710000000000,
        last_submit_ts_ms=1710001234567,
    )

    save_live_state(path, state)

    assert load_live_state(path) == state


@pytest.mark.parametrize(
    "payload",
    [
        "{",
        json.dumps(["not", "an", "object"]),
    ],
)
def test_load_live_state_returns_defaults_for_malformed_or_non_object_payloads(
    tmp_path: Path,
    payload: str,
) -> None:
    path = tmp_path / "live_state.json"
    path.write_text(payload)

    state = load_live_state(path)

    assert state == LiveRuntimeState()


def test_load_live_state_coerces_persisted_values_safely(tmp_path: Path) -> None:
    path = tmp_path / "live_state.json"
    path.write_text(
        json.dumps(
            {
                "last_processed_open_time": "1710000000000",
                "last_submit_ts_ms": "not-an-int",
            }
        )
    )

    state = load_live_state(path)

    assert state == LiveRuntimeState(
        last_processed_open_time=1710000000000,
        last_submit_ts_ms=None,
    )
