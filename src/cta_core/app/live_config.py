from __future__ import annotations

import argparse
from dataclasses import dataclass


@dataclass(frozen=True)
class LiveRunConfig:
    strategy_id: str
    symbol: str
    interval: str
    lookback_bars: int
    poll_seconds: int
    dry_run: bool
    api_key: str
    api_secret: str

    @classmethod
    def build_parser(cls) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description="Run a live trading strategy.")
        parser.add_argument("--strategy", dest="strategy_id", required=True)
        parser.add_argument("--symbol", default="BTCUSDT")
        parser.add_argument("--interval", default="1h")
        parser.add_argument("--lookback-bars", type=int, default=300)
        parser.add_argument("--poll-seconds", type=int, default=2)
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--api-key", default="")
        parser.add_argument("--api-secret", default="")
        return parser

    @classmethod
    def from_argv(cls, argv: list[str] | None) -> LiveRunConfig:
        args = cls.build_parser().parse_args(argv)
        return cls(
            strategy_id=args.strategy_id,
            symbol=args.symbol,
            interval=args.interval,
            lookback_bars=args.lookback_bars,
            poll_seconds=args.poll_seconds,
            dry_run=args.dry_run,
            api_key=args.api_key,
            api_secret=args.api_secret,
        )


__all__ = ["LiveRunConfig"]
