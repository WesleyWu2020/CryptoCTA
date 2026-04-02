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
    taker_buy = [450.0] * 20 + [900.0, 920.0, 940.0, 960.0] + [520.0, 500.0, 480.0, 470.0] + [460.0] * 8
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
            "taker_buy_base_volume": taker_buy,
            "close_time": [((i + 1) * step_ms) - 1 for i in range(len(close))],
        }
    )
    upsert_klines_to_duckdb(db_path=db_path, bars=bars)


def _seed_rp_window_sensitive_duckdb(db_path: Path) -> None:
    close = [10.0, 20.0, 18.0, 19.0, 21.0, 20.0, 18.0]
    step_ms = 86400000
    bars = pl.DataFrame(
        {
            "symbol": ["BTCUSDT"] * len(close),
            "interval": ["1d"] * len(close),
            "open_time": [i * step_ms for i in range(len(close))],
            "open": close,
            "high": [value + 1.0 for value in close],
            "low": [value - 1.0 for value in close],
            "close": close,
            "volume": [1000.0] * len(close),
            "close_time": [((i + 1) * step_ms) - 1 for i in range(len(close))],
        }
    )
    upsert_klines_to_duckdb(db_path=db_path, bars=bars)


def test_list_strategies_prints_registered_ids():
    result = _run_script("--list-strategies")
    assert result.returncode == 0
    assert result.stdout.splitlines() == [
        "rp_daily_breakout",
        "rsi_threshold",
        "sma_cross",
        "liquidity_shock_reversion",
        "taker_imbalance_absorption",
        "liquidation_vacuum_reversion",
        "smart_money_size_breakout",
    ]


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


def test_rp_native_quantity_changes_position_size(tmp_path: Path):
    db_path = tmp_path / "klines.duckdb"
    output_a = tmp_path / "backtest_a.json"
    output_b = tmp_path / "backtest_b.json"
    _seed_rp_window_sensitive_duckdb(db_path)

    result_a = _run_script(
        "--strategy",
        "rp_daily_breakout",
        "--symbol",
        "BTCUSDT",
        "--interval",
        "1d",
        "--start",
        "1970-01-01",
        "--end",
        "1970-01-20",
        "--db-path",
        str(db_path),
        "--output",
        str(output_a),
        "--quantity",
        "1",
    )
    result_b = _run_script(
        "--strategy",
        "rp_daily_breakout",
        "--symbol",
        "BTCUSDT",
        "--interval",
        "1d",
        "--start",
        "1970-01-01",
        "--end",
        "1970-01-20",
        "--db-path",
        str(db_path),
        "--output",
        str(output_b),
        "--quantity",
        "0.2",
    )

    assert result_a.returncode == 0, result_a.stderr
    assert result_b.returncode == 0, result_b.stderr

    payload_a = json.loads(output_a.read_text(encoding="utf-8"))
    payload_b = json.loads(output_b.read_text(encoding="utf-8"))

    assert payload_a["trades"][0]["qty"] != payload_b["trades"][0]["qty"]


def test_rp_window_is_rejected_in_compat_execution(tmp_path: Path):
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
        "--rp-window",
        "5",
    )

    assert result.returncode == 1
    assert result.stdout == ""
    assert "rp_window is not supported in turtle compatibility execution" in result.stderr


def test_unsupported_strategy_execution_fails_clearly():
    result = _run_script("--strategy=sma_cross", "--output", "/tmp/unused.json")
    assert result.returncode == 2
    assert result.stdout == ""
    assert "strategy execution is not yet supported for 'sma_cross'" in result.stderr


def test_rsi_threshold_executes_with_atr_stop_loss_and_writes_output(tmp_path: Path):
    db_path = tmp_path / "klines.duckdb"
    output_path = tmp_path / "rsi_backtest.json"
    _seed_duckdb(db_path)

    result = _run_script(
        "--strategy",
        "rsi_threshold",
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
        "--rsi-window",
        "2",
        "--rsi-buy-threshold",
        "99",
        "--rsi-sell-threshold",
        "100",
        "--disable-trend-filter",
        "--atr-window",
        "2",
        "--atr-stop-multiplier",
        "0.5",
        "--atr-trailing-multiplier",
        "0.5",
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
    assert any(trade["action"] == "EXIT_LONG_STOP" for trade in payload["trades"])


def test_rsi_threshold_supports_adx_max_filter_and_time_stop(tmp_path: Path):
    db_path = tmp_path / "klines.duckdb"
    output_path = tmp_path / "rsi_time_stop.json"
    _seed_duckdb(db_path)

    result = _run_script(
        "--strategy",
        "rsi_threshold",
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
        "--rsi-window",
        "2",
        "--rsi-buy-threshold",
        "99",
        "--rsi-sell-threshold",
        "100",
        "--disable-trend-filter",
        "--enable-adx-filter",
        "--adx-filter-mode",
        "max",
        "--adx-threshold",
        "100",
        "--atr-window",
        "2",
        "--atr-stop-multiplier",
        "100",
        "--atr-trailing-multiplier",
        "100",
        "--max-hold-bars",
        "1",
        "--cooldown-bars",
        "0",
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["config"]["adx_filter_mode"] == "max"
    assert payload["config"]["use_adx_filter"] is True
    assert payload["summary"]["closed_trades"] >= 1
    assert any(trade["action"] == "EXIT_LONG_TIME" for trade in payload["trades"])


def test_rsi_threshold_supports_partial_take_profit(tmp_path: Path):
    db_path = tmp_path / "klines.duckdb"
    output_path = tmp_path / "rsi_partial_tp.json"
    _seed_duckdb(db_path)

    result = _run_script(
        "--strategy",
        "rsi_threshold",
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
        "--rsi-window",
        "2",
        "--rsi-buy-threshold",
        "99",
        "--rsi-sell-threshold",
        "100",
        "--disable-trend-filter",
        "--atr-window",
        "2",
        "--atr-stop-multiplier",
        "2",
        "--atr-trailing-multiplier",
        "2",
        "--enable-partial-take-profit",
        "--take-profit-r-multiple",
        "0.1",
        "--take-profit-fraction",
        "0.5",
        "--cooldown-bars",
        "0",
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["config"]["enable_partial_take_profit"] is True
    assert payload["config"]["take_profit_r_multiple"] == 0.1
    assert payload["config"]["take_profit_fraction"] == 0.5
    assert any(trade["action"] == "TAKE_PROFIT_LONG_RSI" for trade in payload["trades"])


def _seed_liquidity_shock_bars(db_path: Path) -> None:
    step_ms = 900000
    base_close = [100.0 + (0.08 if i % 2 == 0 else -0.08) for i in range(28)]
    close = base_close + [100.1, 92.0, 93.8, 95.2, 96.0, 96.4, 96.2]
    open_ = [close[0], *close[:-1]]
    high = [max(o, c) + 0.5 for o, c in zip(open_, close)]
    low = [min(o, c) - 0.5 for o, c in zip(open_, close)]
    high[29] = 101.5
    low[29] = 80.0
    volume = [100.0] * len(close)
    volume[29] = 1200.0

    bars = pl.DataFrame(
        {
            "symbol": ["BTCUSDT"] * len(close),
            "interval": ["15m"] * len(close),
            "open_time": [i * step_ms for i in range(len(close))],
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "taker_buy_base_volume": [50.0] * len(close),
            "close_time": [((i + 1) * step_ms) - 1 for i in range(len(close))],
        }
    )
    upsert_klines_to_duckdb(db_path=db_path, bars=bars)


def test_liquidity_shock_reversion_executes_and_exits_by_time(tmp_path: Path):
    db_path = tmp_path / "klines.duckdb"
    output_path = tmp_path / "liquidity_shock_reversion.json"
    _seed_liquidity_shock_bars(db_path)

    result = _run_script(
        "--strategy",
        "liquidity_shock_reversion",
        "--symbol",
        "BTCUSDT",
        "--interval",
        "15m",
        "--start",
        "1970-01-01",
        "--end",
        "1970-01-02",
        "--db-path",
        str(db_path),
        "--output",
        str(output_path),
        "--lsr-lookback-bars",
        "20",
        "--lsr-zscore-threshold",
        "2.5",
        "--lsr-volume-quantile",
        "0.9",
        "--lsr-max-hold-bars",
        "2",
        "--lsr-stop-buffer-pct",
        "0.001",
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["symbol"] == "BTCUSDT"
    assert payload["interval"] == "15m"
    assert payload["summary"]["closed_trades"] >= 1
    assert any(trade["action"] == "EXIT_LONG_TIME" for trade in payload["trades"])


def _seed_liquidity_shock_bars_for_tp(db_path: Path) -> None:
    step_ms = 900000
    close = [100.0 + (0.05 if i % 2 == 0 else -0.05) for i in range(28)] + [100.2, 93.0, 94.5, 96.5, 96.0]
    open_ = [close[0], *close[:-1]]
    high = [max(o, c) + 0.4 for o, c in zip(open_, close)]
    low = [min(o, c) - 0.4 for o, c in zip(open_, close)]
    high[29] = 101.0
    low[29] = 80.0
    high[30] = 97.5
    volume = [100.0] * len(close)
    volume[29] = 1500.0

    bars = pl.DataFrame(
        {
            "symbol": ["BTCUSDT"] * len(close),
            "interval": ["15m"] * len(close),
            "open_time": [i * step_ms for i in range(len(close))],
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "taker_buy_base_volume": [50.0] * len(close),
            "close_time": [((i + 1) * step_ms) - 1 for i in range(len(close))],
        }
    )
    upsert_klines_to_duckdb(db_path=db_path, bars=bars)


def test_liquidity_shock_reversion_supports_atr_stop_and_partial_take_profit(tmp_path: Path):
    db_path = tmp_path / "klines.duckdb"
    output_path = tmp_path / "liquidity_shock_reversion_tp.json"
    _seed_liquidity_shock_bars_for_tp(db_path)

    result = _run_script(
        "--strategy",
        "liquidity_shock_reversion",
        "--symbol",
        "BTCUSDT",
        "--interval",
        "15m",
        "--start",
        "1970-01-01",
        "--end",
        "1970-01-02",
        "--db-path",
        str(db_path),
        "--output",
        str(output_path),
        "--lsr-lookback-bars",
        "20",
        "--lsr-zscore-threshold",
        "2.5",
        "--lsr-volume-quantile",
        "0.9",
        "--lsr-max-hold-bars",
        "3",
        "--lsr-stop-mode",
        "atr",
        "--lsr-atr-window",
        "3",
        "--lsr-atr-stop-multiplier",
        "0.5",
        "--lsr-enable-trailing-stop",
        "--lsr-atr-trailing-multiplier",
        "0.5",
        "--lsr-enable-partial-take-profit",
        "--lsr-take-profit-atr-multiple",
        "0.5",
        "--lsr-take-profit-fraction",
        "0.5",
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["config"]["stop_mode"] == "atr"
    assert payload["config"]["enable_partial_take_profit"] is True
    assert any(trade["action"] == "TAKE_PROFIT_LONG_LSR" for trade in payload["trades"])


def _seed_microstructure_strategy_bars(db_path: Path) -> None:
    step_ms = 900000
    close = [100.0 + (0.05 if i % 2 == 0 else -0.05) for i in range(80)]
    open_ = [close[0], *close[:-1]]
    high = [max(o, c) + 0.4 for o, c in zip(open_, close)]
    low = [min(o, c) - 0.4 for o, c in zip(open_, close)]
    volume = [100.0 for _ in close]
    taker_buy = [50.0 for _ in close]
    trades_count = [120 for _ in close]

    open_[40] = 102.0
    high[40] = 104.0
    low[40] = 100.5
    close[40] = 100.8
    volume[40] = 250.0
    taker_buy[40] = 180.0

    open_[50] = 100.0
    high[50] = 100.2
    low[50] = 97.0
    close[50] = 98.8
    volume[50] = 300.0
    taker_buy[50] = 60.0

    open_[60] = 100.0
    high[60] = 101.7
    low[60] = 99.9
    close[60] = 101.5
    volume[60] = 400.0
    taker_buy[60] = 260.0
    trades_count[60] = 50

    bars = pl.DataFrame(
        {
            "symbol": ["BTCUSDT"] * len(close),
            "interval": ["15m"] * len(close),
            "open_time": [i * step_ms for i in range(len(close))],
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "trades_count": trades_count,
            "taker_buy_base_volume": taker_buy,
            "close_time": [((i + 1) * step_ms) - 1 for i in range(len(close))],
        }
    )
    upsert_klines_to_duckdb(db_path=db_path, bars=bars)


@pytest.mark.parametrize(
    ("strategy", "extra_args"),
    [
        (
            "taker_imbalance_absorption",
            [
                "--tia-volume-ma-window",
                "20",
                "--tia-min-taker-buy-ratio",
                "0.65",
                "--tia-close-location-max",
                "0.4",
                "--tia-max-hold-bars",
                "2",
            ],
        ),
        (
            "liquidation_vacuum_reversion",
            [
                "--lvr-volume-peak-window",
                "48",
                "--lvr-min-range-pct",
                "0.015",
                "--lvr-min-taker-sell-ratio",
                "0.7",
                "--lvr-max-hold-bars",
                "2",
            ],
        ),
        (
            "smart_money_size_breakout",
            [
                "--smb-avg-trade-size-window",
                "24",
                "--smb-size-zscore-threshold",
                "2.0",
                "--smb-min-taker-buy-ratio",
                "0.55",
                "--smb-close-to-high-threshold",
                "0.8",
                "--smb-exit-buy-ratio-threshold",
                "0.5",
                "--smb-max-hold-bars",
                "2",
            ],
        ),
    ],
)
def test_microstructure_strategies_execute(strategy: str, extra_args: list[str], tmp_path: Path):
    db_path = tmp_path / "klines.duckdb"
    output_path = tmp_path / f"{strategy}.json"
    _seed_microstructure_strategy_bars(db_path)

    result = _run_script(
        "--strategy",
        strategy,
        "--symbol",
        "BTCUSDT",
        "--interval",
        "15m",
        "--start",
        "1970-01-01",
        "--end",
        "1970-01-02",
        "--db-path",
        str(db_path),
        "--output",
        str(output_path),
        *extra_args,
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["summary"]["closed_trades"] >= 1


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


def _seed_legacy_schema_without_microstructure_columns(db_path: Path) -> None:
    import duckdb

    step_ms = 900000
    close = [100.0 + (0.2 if i % 2 == 0 else -0.2) for i in range(40)]
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE futures_klines (
              symbol VARCHAR NOT NULL,
              interval VARCHAR NOT NULL,
              open_time BIGINT NOT NULL,
              open DOUBLE NOT NULL,
              high DOUBLE NOT NULL,
              low DOUBLE NOT NULL,
              close DOUBLE NOT NULL,
              volume DOUBLE NOT NULL,
              close_time BIGINT NOT NULL
            )
            """
        )
        rows = [
            (
                "BTCUSDT",
                "15m",
                i * step_ms,
                close[i - 1] if i > 0 else close[0],
                close[i] + 0.5,
                close[i] - 0.5,
                close[i],
                100.0,
                ((i + 1) * step_ms) - 1,
            )
            for i in range(len(close))
        ]
        conn.executemany(
            """
            INSERT INTO futures_klines (
              symbol, interval, open_time, open, high, low, close, volume, close_time
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
    finally:
        conn.close()


def test_microstructure_strategy_handles_legacy_schema_without_taker_columns(tmp_path: Path):
    db_path = tmp_path / "legacy.duckdb"
    output_path = tmp_path / "legacy_tia.json"
    _seed_legacy_schema_without_microstructure_columns(db_path)

    result = _run_script(
        "--strategy",
        "taker_imbalance_absorption",
        "--symbol",
        "BTCUSDT",
        "--interval",
        "15m",
        "--start",
        "1970-01-01",
        "--end",
        "1970-01-02",
        "--db-path",
        str(db_path),
        "--output",
        str(output_path),
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["summary"]["closed_trades"] == 0


def test_runner_handles_existing_duckdb_without_futures_table(tmp_path: Path):
    import duckdb

    db_path = tmp_path / "empty.duckdb"
    output_path = tmp_path / "empty_result.json"
    conn = duckdb.connect(str(db_path))
    conn.close()

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
        "1970-01-10",
        "--db-path",
        str(db_path),
        "--output",
        str(output_path),
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["summary"]["closed_trades"] == 0


def _seed_rsi_same_bar_tp_and_stop(db_path: Path) -> None:
    step_ms = 86400000
    bars = pl.DataFrame(
        {
            "symbol": ["BTCUSDT"] * 10,
            "interval": ["1d"] * 10,
            "open_time": [i * step_ms for i in range(10)],
            "open": [100.0, 99.0, 98.0, 97.0, 96.0, 95.0, 95.0, 95.0, 95.0, 95.0],
            "high": [101.0, 100.0, 99.0, 98.0, 97.0, 96.0, 96.0, 110.0, 96.0, 96.0],
            "low": [99.0, 98.0, 97.0, 96.0, 95.0, 94.0, 94.0, 90.0, 94.0, 94.0],
            "close": [99.0, 98.0, 97.0, 96.0, 95.0, 95.0, 95.0, 95.0, 95.0, 95.0],
            "volume": [1000.0] * 10,
            "taker_buy_base_volume": [500.0] * 10,
            "close_time": [((i + 1) * step_ms) - 1 for i in range(10)],
        }
    )
    upsert_klines_to_duckdb(db_path=db_path, bars=bars)


def test_rsi_prefers_stop_over_take_profit_when_both_trigger_on_signal_bar(tmp_path: Path):
    db_path = tmp_path / "rsi_same_bar.duckdb"
    output_path = tmp_path / "rsi_same_bar.json"
    _seed_rsi_same_bar_tp_and_stop(db_path)

    result = _run_script(
        "--strategy",
        "rsi_threshold",
        "--symbol",
        "BTCUSDT",
        "--interval",
        "1d",
        "--start",
        "1970-01-01",
        "--end",
        "1970-01-11",
        "--db-path",
        str(db_path),
        "--output",
        str(output_path),
        "--rsi-window",
        "2",
        "--rsi-buy-threshold",
        "99",
        "--rsi-sell-threshold",
        "100",
        "--disable-trend-filter",
        "--atr-window",
        "2",
        "--atr-stop-multiplier",
        "0.5",
        "--atr-trailing-multiplier",
        "0.5",
        "--enable-partial-take-profit",
        "--take-profit-r-multiple",
        "0.1",
        "--take-profit-fraction",
        "0.5",
        "--cooldown-bars",
        "0",
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    stop_times = {
        int(trade["open_time"])
        for trade in payload["trades"]
        if trade["action"] == "EXIT_LONG_STOP"
    }
    tp_times = {
        int(trade["open_time"])
        for trade in payload["trades"]
        if trade["action"] == "TAKE_PROFIT_LONG_RSI"
    }
    assert stop_times
    assert tp_times.isdisjoint(stop_times)
