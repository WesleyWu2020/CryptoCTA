from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RunConfig:
    symbol: str = "BTCUSDT"
    interval: str = "1d"
    start: str = "2024-09-01"
    end: str = "2026-03-16"
    db_path: Path = Path("artifacts/market_data/klines.duckdb")
    output: Path = Path("artifacts/backtests/result.json")
    use_binance: bool = False
    initial_capital: float = 100_000.0
    fee_bps: float = 5.0
    slippage_bps: float = 1.0
    max_leverage: float = 1.0

    @classmethod
    def register_cli_args(cls, parser: argparse.ArgumentParser) -> None:
        parser.add_argument("--symbol", default="BTCUSDT")
        parser.add_argument("--interval", default="1d")
        parser.add_argument("--start", default="2024-09-01")
        parser.add_argument(
            "--end",
            default="2026-03-16",
            help="exclusive date; use 2026-03-16 for up to 2026-03-15",
        )
        parser.add_argument(
            "--db-path",
            type=Path,
            default=Path("artifacts/market_data/klines.duckdb"),
        )
        parser.add_argument(
            "--output",
            type=Path,
            default=Path("artifacts/backtests/btcusdt_1d_rp_break_2024-09-01_2026-03-15_fee5.json"),
        )
        parser.add_argument(
            "--use-binance",
            action="store_true",
            help="force fetch from Binance API instead of DuckDB",
        )
        parser.add_argument("--initial-capital", type=float, default=100_000.0)
        parser.add_argument("--fee-bps", type=float, default=5.0)
        parser.add_argument("--slippage-bps", type=float, default=1.0)
        parser.add_argument("--max-leverage", type=float, default=1.0)

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> RunConfig:
        return cls(
            symbol=args.symbol,
            interval=args.interval,
            start=args.start,
            end=args.end,
            db_path=args.db_path,
            output=args.output,
            use_binance=args.use_binance,
            initial_capital=args.initial_capital,
            fee_bps=args.fee_bps,
            slippage_bps=args.slippage_bps,
            max_leverage=args.max_leverage,
        )


__all__ = ["RunConfig"]
