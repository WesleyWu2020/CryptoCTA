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


def test_append_closed_bars_dedupes_and_sorts(tmp_path: Path):
    path = tmp_path / "bars.parquet"

    initial = normalize_klines(
        symbol="BTCUSDT",
        interval="15m",
        rows=[
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
            ],
            [
                1700000900000,
                "62400",
                "62600",
                "62300",
                "62500",
                "111.1",
                1700001799999,
                "0",
                0,
                "0",
                "0",
                "0",
            ],
        ],
    )
    append_closed_bars(initial, path)

    duplicate = normalize_klines(
        symbol="BTCUSDT",
        interval="15m",
        rows=[
            [
                1700000000000,
                "62111",
                "62555",
                "61777",
                "62444",
                "222.2",
                1700000899999,
                "0",
                0,
                "0",
                "0",
                "0",
            ]
        ],
    )
    append_closed_bars(duplicate, path)

    loaded = pl.read_parquet(path)
    assert loaded.height == 2
    assert loaded.select("open_time").to_series().to_list() == [
        1700000000000,
        1700000900000,
    ]
    assert loaded.filter(pl.col("open_time") == 1700000000000).select("close").item() == 62444.0


def test_append_closed_bars_dedupes_duplicates_within_single_batch(tmp_path: Path):
    path = tmp_path / "bars.parquet"

    batch = normalize_klines(
        symbol="BTCUSDT",
        interval="15m",
        rows=[
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
            ],
            [
                1700000000000,
                "62111",
                "62555",
                "61777",
                "62444",
                "222.2",
                1700000899999,
                "0",
                0,
                "0",
                "0",
                "0",
            ],
        ],
    )

    append_closed_bars(batch, path)

    loaded = pl.read_parquet(path)
    assert loaded.height == 1
    assert loaded.select("open_time").to_series().to_list() == [1700000000000]
    assert loaded.select("close").item() == 62444.0
