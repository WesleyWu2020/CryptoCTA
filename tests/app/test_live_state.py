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


def test_load_live_state_rejects_non_integral_float_values(tmp_path: Path) -> None:
    path = tmp_path / "live_state.json"
    path.write_text(
        json.dumps(
            {
                "last_processed_open_time": 1.9,
                "last_submit_ts_ms": 2.0,
            }
        )
    )

    state = load_live_state(path)

    assert state == LiveRuntimeState(
        last_processed_open_time=None,
        last_submit_ts_ms=2,
    )


def test_load_live_state_returns_defaults_for_non_utf8_bytes(tmp_path: Path) -> None:
    path = tmp_path / "live_state.json"
    path.write_bytes(b"\xff\xfe\x00")

    state = load_live_state(path)

    assert state == LiveRuntimeState()


def test_save_live_state_uses_atomic_replace(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import cta_core.app.live_state as live_state_module

    calls: list[tuple[Path, Path]] = []

    def fake_replace(src: str | bytes | Path, dst: str | bytes | Path) -> None:
        calls.append((Path(src), Path(dst)))

    monkeypatch.setattr(live_state_module.os, "replace", fake_replace)

    state = LiveRuntimeState(last_processed_open_time=1, last_submit_ts_ms=2)
    path = tmp_path / "nested" / "live_state.json"

    save_live_state(path, state)

    assert len(calls) == 1
    assert calls[0][1] == path
    assert calls[0][0] != path
    assert calls[0][0].parent == path.parent
