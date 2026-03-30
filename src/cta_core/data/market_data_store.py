from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from cta_core.data.ingest import normalize_klines


def utc_ms(date_str: str) -> int:
    dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def fetch_klines_range(
    *,
    client: Any,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int = 1500,
) -> pl.DataFrame:
    all_rows: list[list[object]] = []
    cursor = start_ms

    while cursor < end_ms:
        batch = client.fetch_klines(
            symbol=symbol,
            interval=interval,
            limit=limit,
            start_time=cursor,
            end_time=end_ms,
        )
        if not batch:
            break

        all_rows.extend(batch)
        last_open_time = int(batch[-1][0])

        if last_open_time < cursor:
            break
        cursor = last_open_time + 1

        if len(batch) < limit:
            break

    if not all_rows:
        return pl.DataFrame(
            {
                "symbol": [],
                "interval": [],
                "open_time": [],
                "open": [],
                "high": [],
                "low": [],
                "close": [],
                "volume": [],
                "close_time": [],
            }
        )

    bars = normalize_klines(symbol=symbol, interval=interval, rows=all_rows)
    bars = bars.filter((pl.col("open_time") >= start_ms) & (pl.col("open_time") < end_ms))
    return bars.unique(subset=["open_time"], keep="first").sort("open_time")


def upsert_klines_to_duckdb(*, db_path: Path, bars: pl.DataFrame) -> int:
    if bars.height == 0:
        return 0

    required_columns = {
        "symbol",
        "interval",
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
    }
    missing = sorted(required_columns - set(bars.columns))
    if missing:
        raise ValueError(f"bars missing required columns: {', '.join(missing)}")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(db_path))
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS futures_klines (
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
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_futures_klines_symbol_interval_open_time
            ON futures_klines(symbol, interval, open_time)
            """
        )

        conn.register("incoming", bars.to_arrow())
        conn.execute(
            """
            DELETE FROM futures_klines AS existing
            USING incoming
            WHERE existing.symbol = incoming.symbol
              AND existing.interval = incoming.interval
              AND existing.open_time = incoming.open_time
            """
        )
        conn.execute(
            """
            INSERT INTO futures_klines (
              symbol, interval, open_time, open, high, low, close, volume, close_time
            )
            SELECT
              symbol, interval, open_time, open, high, low, close, volume, close_time
            FROM incoming
            """
        )

        return bars.height
    finally:
        conn.close()
