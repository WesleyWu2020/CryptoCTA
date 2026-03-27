from __future__ import annotations

from typing import Sequence

import polars as pl

KlineRow = Sequence[object]


def normalize_klines(symbol: str, interval: str, rows: list[KlineRow]) -> pl.DataFrame:
    frame = pl.DataFrame(
        rows,
        schema=[
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_asset_volume",
            "trade_count",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
            "ignore",
        ],
        orient="row",
    )
    return (
        frame.select(
            pl.lit(symbol).alias("symbol"),
            pl.lit(interval).alias("interval"),
            pl.col("open_time").cast(pl.Int64),
            pl.col("open").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("volume").cast(pl.Float64),
            pl.col("close_time").cast(pl.Int64),
        )
        .select(
            "symbol",
            "interval",
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
        )
    )
