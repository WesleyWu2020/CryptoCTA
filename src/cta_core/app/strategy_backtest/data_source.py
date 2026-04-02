from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl

from cta_core.data.binance_client import BinanceUMClient
from cta_core.data.market_data_store import fetch_klines_range


def _empty_bars() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "open_time": [],
            "open": [],
            "high": [],
            "low": [],
            "close": [],
            "volume": [],
            "taker_buy_base_volume": [],
            "trades_count": [],
        }
    )


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
        try:
            columns = conn.execute("PRAGMA table_info('futures_klines')").fetchall()
        except duckdb.CatalogException:
            return _empty_bars()
        column_names = {str(column[1]) for column in columns}
        has_taker_base = "taker_buy_base" in column_names
        has_taker_base_volume = "taker_buy_base_volume" in column_names
        has_trades_count = "trades_count" in column_names
        has_trade_count = "trade_count" in column_names
        if has_taker_base and has_taker_base_volume:
            taker_expr = "COALESCE(taker_buy_base, taker_buy_base_volume, 0.0) AS taker_buy_base_volume"
        elif has_taker_base:
            taker_expr = "COALESCE(taker_buy_base, 0.0) AS taker_buy_base_volume"
        elif has_taker_base_volume:
            taker_expr = "COALESCE(taker_buy_base_volume, 0.0) AS taker_buy_base_volume"
        else:
            taker_expr = "0.0 AS taker_buy_base_volume"
        if has_trades_count and has_trade_count:
            trades_expr = "COALESCE(trades_count, trade_count, 0) AS trades_count"
        elif has_trades_count:
            trades_expr = "COALESCE(trades_count, 0) AS trades_count"
        elif has_trade_count:
            trades_expr = "COALESCE(trade_count, 0) AS trades_count"
        else:
            trades_expr = "0 AS trades_count"
        table = conn.execute(
            f"""
            SELECT open_time, open, high, low, close, volume, {taker_expr}, {trades_expr}
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
    ).select("open_time", "open", "high", "low", "close", "volume", "taker_buy_base_volume", "trades_count")
    return bars, "binance_api"


__all__ = ["load_bars_from_duckdb", "load_or_fetch"]
