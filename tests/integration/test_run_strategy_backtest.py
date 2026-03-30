from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "run_strategy_backtest.py"


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


def test_list_strategies_prints_registered_ids():
    result = _run_script("--list-strategies")
    assert result.returncode == 0
    assert result.stdout.splitlines() == ["rp_daily_breakout", "sma_cross"]


def test_strategy_prints_selected_strategy_id():
    result = _run_script("--strategy", "rp_daily_breakout")
    assert result.returncode == 0
    assert result.stdout.strip() == "rp_daily_breakout"


def test_legacy_strategy_prints_selected_strategy_id():
    result = _run_script("--strategy", "sma_cross")
    assert result.returncode == 0
    assert result.stdout.strip() == "sma_cross"
