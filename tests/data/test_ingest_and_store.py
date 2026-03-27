from pathlib import Path

import polars as pl

from cta_core.data.ingest import normalize_klines
from cta_core.data.parquet_store import append_closed_bars


def test_normalize_and_append(tmp_path: Path):
    raw = [
        [
            1700000000000,
            "62000",
            "62500",
            "61800",
            "62400",
            "123.4",
            1700000899999,
            "0",
            0,
            "0",
            "0",
            "0",
        ]
    ]
    df = normalize_klines(symbol="BTCUSDT", interval="15m", rows=raw)
    assert df.select("symbol").item() == "BTCUSDT"
    path = tmp_path / "bars.parquet"
    append_closed_bars(df, path)
    loaded = pl.read_parquet(path)
    assert loaded.height == 1
