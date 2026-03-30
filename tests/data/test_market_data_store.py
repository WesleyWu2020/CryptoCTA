from pathlib import Path

import duckdb
import polars as pl

from cta_core.data.market_data_store import fetch_klines_range, upsert_klines_to_duckdb


class _FakeClient:
    def __init__(self, batches):
        self._batches = list(batches)

    def fetch_klines(self, symbol, interval, limit, start_time, end_time):
        if not self._batches:
            return []
        return self._batches.pop(0)


def _row(open_time: int, open_: str, high: str, low: str, close: str):
    close_time = open_time + 899999
    return [open_time, open_, high, low, close, "10", close_time, "0", 0, "0", "0", "0"]


def test_fetch_klines_range_merges_pages_and_dedupes():
    client = _FakeClient(
        batches=[
            [
                _row(1000, "1", "2", "0.5", "1.5"),
                _row(2000, "2", "3", "1.5", "2.5"),
            ],
            [
                _row(2000, "2", "3", "1.5", "2.6"),
                _row(3000, "3", "4", "2.5", "3.5"),
            ],
        ]
    )

    bars = fetch_klines_range(
        client=client,
        symbol="BTCUSDT",
        interval="15m",
        start_ms=1000,
        end_ms=4000,
        limit=2,
    )

    assert bars.select("open_time").to_series().to_list() == [1000, 2000, 3000]
    assert bars.filter(pl.col("open_time") == 2000).select("close").item() == 2.5


def test_upsert_klines_to_duckdb_updates_existing_rows(tmp_path: Path):
    db_path = tmp_path / "klines.duckdb"

    first = pl.DataFrame(
        {
            "symbol": ["BTCUSDT"],
            "interval": ["15m"],
            "open_time": [1000],
            "open": [1.0],
            "high": [2.0],
            "low": [0.5],
            "close": [1.5],
            "volume": [10.0],
            "close_time": [1999],
        }
    )
    second = pl.DataFrame(
        {
            "symbol": ["BTCUSDT"],
            "interval": ["15m"],
            "open_time": [1000],
            "open": [1.0],
            "high": [2.0],
            "low": [0.5],
            "close": [1.8],
            "volume": [11.0],
            "close_time": [1999],
        }
    )

    upsert_klines_to_duckdb(db_path=db_path, bars=first)
    upsert_klines_to_duckdb(db_path=db_path, bars=second)

    conn = duckdb.connect(str(db_path))
    rows = conn.execute(
        "SELECT symbol, interval, open_time, close, volume FROM futures_klines"
    ).fetchall()
    conn.close()

    assert len(rows) == 1
    assert rows[0] == ("BTCUSDT", "15m", 1000, 1.8, 11.0)
