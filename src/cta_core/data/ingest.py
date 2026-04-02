from __future__ import annotations

from typing import Sequence

import polars as pl

KlineRow = Sequence[object]


def _infer_base_asset(symbol: str) -> str:
    known_quotes = ("USDT", "USDC", "BUSD", "FDUSD", "BTC", "ETH", "BNB")
    for quote in known_quotes:
        if symbol.endswith(quote) and len(symbol) > len(quote):
            return symbol[: -len(quote)]
    return symbol


def normalize_klines(symbol: str, interval: str, rows: list[KlineRow]) -> pl.DataFrame:
    base_asset = _infer_base_asset(symbol)
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
            pl.lit(base_asset).alias("base_asset"),
            pl.lit(interval).alias("interval"),
            pl.col("open_time").cast(pl.Int64),
            pl.col("open").cast(pl.Float64),
            pl.col("high").cast(pl.Float64),
            pl.col("low").cast(pl.Float64),
            pl.col("close").cast(pl.Float64),
            pl.col("volume").cast(pl.Float64),
            pl.col("quote_asset_volume").cast(pl.Float64).alias("quote_volume"),
            pl.col("trade_count").cast(pl.Int64).alias("trades_count"),
            pl.col("taker_buy_base_volume").cast(pl.Float64).alias("taker_buy_base"),
            pl.col("taker_buy_quote_volume").cast(pl.Float64).alias("taker_buy_quote"),
            pl.col("close_time").cast(pl.Int64),
        )
        .with_columns(
            pl.from_epoch(pl.col("open_time"), time_unit="ms")
            .dt.replace_time_zone("UTC")
            .alias("datetime"),
            pl.from_epoch(pl.col("open_time"), time_unit="ms").dt.date().alias("date"),
            pl.col("taker_buy_base").alias("taker_buy_base_volume"),
            pl.col("taker_buy_quote").alias("taker_buy_quote_volume"),
        )
        .select(
            "symbol",
            "base_asset",
            "interval",
            "date",
            "datetime",
            "open_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "trades_count",
            "taker_buy_base",
            "taker_buy_quote",
            "taker_buy_base_volume",
            "taker_buy_quote_volume",
            "close_time",
        )
    )
