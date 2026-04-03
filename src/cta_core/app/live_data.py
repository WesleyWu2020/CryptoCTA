from __future__ import annotations

import time
from typing import Any

import polars as pl

from cta_core.data.ingest import normalize_klines


def _empty_normalized_frame(symbol: str, interval: str) -> pl.DataFrame:
    return normalize_klines(symbol=symbol, interval=interval, rows=[])


def fetch_closed_bars(
    client: Any,
    symbol: str,
    interval: str,
    lookback_bars: int,
    now_ms: int | None = None,
) -> pl.DataFrame:
    if lookback_bars <= 0:
        raise ValueError("lookback_bars must be > 0")

    rows = client.fetch_klines(
        symbol=symbol,
        interval=interval,
        limit=max(lookback_bars + 2, 10),
    )
    if not rows:
        return _empty_normalized_frame(symbol, interval)

    try:
        bars = normalize_klines(symbol=symbol, interval=interval, rows=rows).sort("open_time")
    except Exception:
        return _empty_normalized_frame(symbol, interval)

    cutoff_ms = now_ms if now_ms is not None else time.time_ns() // 1_000_000
    closed = bars.filter(pl.col("close_time") <= cutoff_ms)
    return closed.tail(lookback_bars)


def select_new_closed_bars(bars: pl.DataFrame, last_processed_open_time: int | None) -> pl.DataFrame:
    if last_processed_open_time is None:
        return bars
    return bars.filter(pl.col("open_time") > last_processed_open_time)


__all__ = ["fetch_closed_bars", "select_new_closed_bars"]
