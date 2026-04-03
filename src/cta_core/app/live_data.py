from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import polars as pl

from cta_core.data.ingest import normalize_klines


def fetch_closed_bars(
    client: Any,
    symbol: str,
    interval: str,
    lookback_bars: int,
    now_ms: int | None = None,
) -> pl.DataFrame:
    rows = client.fetch_klines(
        symbol=symbol,
        interval=interval,
        limit=max(lookback_bars + 2, 10),
    )
    if not rows:
        return normalize_klines(symbol=symbol, interval=interval, rows=[])

    bars = normalize_klines(symbol=symbol, interval=interval, rows=rows).sort("open_time")
    cutoff_ms = now_ms
    if cutoff_ms is None:
        cutoff_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    closed = bars.filter(pl.col("close_time") <= cutoff_ms)
    return closed.tail(lookback_bars)


def select_new_closed_bars(bars: pl.DataFrame, last_processed_open_time: int | None) -> pl.DataFrame:
    if last_processed_open_time is None:
        return bars
    return bars.filter(pl.col("open_time") > last_processed_open_time)


__all__ = ["fetch_closed_bars", "select_new_closed_bars"]
