from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from cta_core.data.binance_client import BinanceUMClient
from cta_core.data.market_data_store import fetch_klines_range, upsert_klines_to_duckdb, utc_ms


def _parse_csv_arg(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def _print_coverage(db_path: Path, symbols: list[str], intervals: list[str]) -> None:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        rows = conn.execute(
            """
            SELECT symbol, interval, COUNT(*) AS rows, MIN(open_time) AS min_open_time, MAX(open_time) AS max_open_time
            FROM futures_klines
            WHERE symbol IN ({symbols})
              AND interval IN ({intervals})
            GROUP BY symbol, interval
            ORDER BY symbol, interval
            """.format(
                symbols=", ".join([f"'{s}'" for s in symbols]),
                intervals=", ".join([f"'{i}'" for i in intervals]),
            )
        ).fetchall()
    finally:
        conn.close()

    for row in rows:
        print(
            f"symbol={row[0]} interval={row[1]} rows={row[2]} "
            f"min_open_time={row[3]} max_open_time={row[4]}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest Binance UM futures klines into DuckDB.")
    parser.add_argument("--symbols", default="BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT")
    parser.add_argument("--intervals", default="15m,1h")
    parser.add_argument("--start", default="2024-09-01")
    parser.add_argument(
        "--end",
        default="2026-03-29",
        help="exclusive date, default includes data up to 2026-03-28",
    )
    parser.add_argument("--db-path", type=Path, default=Path("artifacts/market_data/klines.duckdb"))
    args = parser.parse_args()

    symbols = _parse_csv_arg(args.symbols)
    intervals = _parse_csv_arg(args.intervals)
    start_ms = utc_ms(args.start)
    end_ms = utc_ms(args.end)

    client = BinanceUMClient()
    total_rows = 0

    for symbol in symbols:
        for interval in intervals:
            print(f"fetching {symbol} {interval} from {args.start} to {args.end} (exclusive)")
            bars = fetch_klines_range(
                client=client,
                symbol=symbol,
                interval=interval,
                start_ms=start_ms,
                end_ms=end_ms,
                limit=1500,
            )
            written = upsert_klines_to_duckdb(db_path=args.db_path, bars=bars)
            total_rows += written
            print(f"written_rows={written}")

    print(f"db_path={args.db_path}")
    print(f"total_written_rows={total_rows}")
    _print_coverage(args.db_path, symbols, intervals)


if __name__ == "__main__":
    main()
