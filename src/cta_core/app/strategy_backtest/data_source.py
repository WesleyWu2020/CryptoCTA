from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

from cta_core.data.binance_client import BinanceUMClient
from cta_core.data.market_data_store import fetch_klines_range


def load_bars_from_duckdb(
    *,
    db_path: Path,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
) -> pl.DataFrame:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        table = conn.execute(
            """
            SELECT open_time, open, high, low, close, volume
            FROM futures_klines
            WHERE symbol = ?
              AND interval = ?
              AND open_time >= ?
              AND open_time < ?
            ORDER BY open_time
            """,
            [symbol, interval, start_ms, end_ms],
        ).to_arrow_table()
    finally:
        conn.close()
    return pl.from_arrow(table)


def load_or_fetch(
    *,
    db_path: Path,
    use_binance: bool,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
) -> tuple[pl.DataFrame, str]:
    if (not use_binance) and db_path.exists():
        return (
            load_bars_from_duckdb(
                db_path=db_path,
                symbol=symbol,
                interval=interval,
                start_ms=start_ms,
                end_ms=end_ms,
            ),
            f"duckdb:{db_path}",
        )

    client = BinanceUMClient()
    bars = fetch_klines_range(
        client=client,
        symbol=symbol,
        interval=interval,
        start_ms=start_ms,
        end_ms=end_ms,
        limit=1500,
    ).select("open_time", "open", "high", "low", "close", "volume")
    return bars, "binance_api"


__all__ = ["load_bars_from_duckdb", "load_or_fetch"]
