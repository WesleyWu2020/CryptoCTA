from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import polars as pl
import pytest

from cta_core.data.market_data_store import upsert_klines_to_duckdb


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "run_strategy_backtest.py"
TURTLE_SCRIPT = ROOT / "scripts" / "run_turtle_backtest.py"


def _run_script(*args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT / "src")
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def _run_turtle_script(*args: str) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT / "src")
    return subprocess.run(
        [sys.executable, str(TURTLE_SCRIPT), *args],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def _seed_duckdb(db_path: Path) -> None:
    close = [100.0] * 20 + [103.0, 106.0, 109.0, 111.0] + [108.0, 104.0, 99.0, 95.0] + [95.0] * 8
    step_ms = 86400000
    bars = pl.DataFrame(
        {
            "symbol": ["BTCUSDT"] * len(close),
            "interval": ["1d"] * len(close),
            "open_time": [i * step_ms for i in range(len(close))],
            "open": close,
            "high": [value + 2.0 for value in close],
            "low": [value - 2.0 for value in close],
            "close": close,
            "volume": [1000.0] * len(close),
            "close_time": [((i + 1) * step_ms) - 1 for i in range(len(close))],
        }
    )
    upsert_klines_to_duckdb(db_path=db_path, bars=bars)


def test_list_strategies_prints_registered_ids():
    result = _run_script("--list-strategies")
    assert result.returncode == 0
    assert result.stdout.splitlines() == ["rp_daily_breakout", "sma_cross"]


def test_strategy_prints_selected_strategy_id():
    result = _run_script("--strategy", "rp_daily_breakout")
    assert result.returncode == 0
    assert result.stdout.strip() == "rp_daily_breakout"


def test_strategy_equals_form_prints_selected_strategy_id():
    result = _run_script("--strategy=rp_daily_breakout")
    assert result.returncode == 0
    assert result.stdout.strip() == "rp_daily_breakout"


def test_legacy_strategy_prints_selected_strategy_id():
    result = _run_script("--strategy", "sma_cross")
    assert result.returncode == 0
    assert result.stdout.strip() == "sma_cross"


def test_legacy_strategy_equals_form_prints_selected_strategy_id():
    result = _run_script("--strategy=sma_cross")
    assert result.returncode == 0
    assert result.stdout.strip() == "sma_cross"


def test_rp_daily_breakout_executes_and_writes_output(tmp_path: Path):
    db_path = tmp_path / "klines.duckdb"
    output_path = tmp_path / "backtest.json"
    _seed_duckdb(db_path)

    result = _run_script(
        "--strategy",
        "rp_daily_breakout",
        "--symbol",
        "BTCUSDT",
        "--interval",
        "1d",
        "--start",
        "1970-01-01",
        "--end",
        "1970-02-10",
        "--db-path",
        str(db_path),
        "--output",
        str(output_path),
        "--rp-entry-confirm-bars",
        "1",
        "--rp-exit-confirm-bars",
        "1",
        "--cooldown-bars",
        "0",
    )

    assert result.returncode == 0, result.stderr
    assert output_path.exists()

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["symbol"] == "BTCUSDT"
    assert payload["interval"] == "1d"
    assert payload["data_source"] == f"duckdb:{db_path}"
    assert payload["summary"]["closed_trades"] >= 1


def test_unsupported_strategy_execution_fails_clearly():
    result = _run_script("--strategy=sma_cross", "--output", "/tmp/unused.json")
    assert result.returncode == 2
    assert result.stdout == ""
    assert "strategy execution is not yet supported for 'sma_cross'" in result.stderr


@pytest.mark.parametrize(
    ("extra_args", "expected_option"),
    [
        (["--disable-htf-filter"], "--disable-htf-filter"),
        (["--htf-interval", "4h"], "--htf-interval"),
    ],
)
def test_rp_daily_breakout_rejects_unsupported_htf_execution_options(
    extra_args: list[str],
    expected_option: str,
):
    result = _run_script("--strategy=rp_daily_breakout", *extra_args)
    assert result.returncode == 2
    assert result.stdout == ""
    assert "HTF execution options are not yet supported by the generic runner" in result.stderr
    assert expected_option in result.stderr


def test_turtle_script_forwards_equals_style_strategy_argument(tmp_path: Path):
    db_path = tmp_path / "klines.duckdb"
    output_path = tmp_path / "forwarded.json"
    _seed_duckdb(db_path)

    result = _run_turtle_script(
        "--strategy=rp_live",
        "--symbol",
        "BTCUSDT",
        "--interval",
        "1d",
        "--start",
        "1970-01-01",
        "--end",
        "1970-02-10",
        "--db-path",
        str(db_path),
        "--output",
        str(output_path),
        "--rp-entry-confirm-bars",
        "1",
        "--rp-exit-confirm-bars",
        "1",
        "--cooldown-bars",
        "0",
    )

    assert result.returncode == 0, result.stderr
    assert output_path.exists()

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["symbol"] == "BTCUSDT"
    assert payload["data_source"] == f"duckdb:{db_path}"
    assert payload["summary"]["closed_trades"] >= 1


def test_turtle_script_strips_user_preset_override(tmp_path: Path):
    db_path = tmp_path / "klines.duckdb"
    output_path = tmp_path / "preset_stripped.json"
    _seed_duckdb(db_path)

    result = _run_turtle_script(
        "--strategy=rp_live",
        "--preset=unknown_preset",
        "--symbol",
        "BTCUSDT",
        "--interval",
        "1d",
        "--start",
        "1970-01-01",
        "--end",
        "1970-02-10",
        "--db-path",
        str(db_path),
        "--output",
        str(output_path),
        "--rp-entry-confirm-bars",
        "1",
        "--rp-exit-confirm-bars",
        "1",
        "--cooldown-bars",
        "0",
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["symbol"] == "BTCUSDT"
    assert payload["data_source"] == f"duckdb:{db_path}"


def test_turtle_script_rejects_unknown_legacy_strategy():
    result = _run_turtle_script("--strategy", "unknown_strategy")
    assert result.returncode == 2
    assert "invalid choice" in result.stderr
