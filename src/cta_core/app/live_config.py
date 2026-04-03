from __future__ import annotations

import argparse
from dataclasses import dataclass
from decimal import Decimal


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
    state_path: str
    max_daily_loss: Decimal
    max_losing_streak: int
    max_symbol_notional_ratio: Decimal
    max_leverage: Decimal
    fee_bps: Decimal
    max_cycles: int | None

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
        parser.add_argument("--state-path", default="artifacts/live_state/rp_daily_breakout.json")
        parser.add_argument("--max-daily-loss", type=Decimal, default=Decimal("500"))
        parser.add_argument("--max-losing-streak", type=int, default=3)
        parser.add_argument("--max-symbol-notional-ratio", type=Decimal, default=Decimal("0.4"))
        parser.add_argument("--max-leverage", type=Decimal, default=Decimal("1"))
        parser.add_argument("--fee-bps", type=Decimal, default=Decimal("5"))
        parser.add_argument("--max-cycles", type=int, default=None)
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
            state_path=args.state_path,
            max_daily_loss=args.max_daily_loss,
            max_losing_streak=args.max_losing_streak,
            max_symbol_notional_ratio=args.max_symbol_notional_ratio,
            max_leverage=args.max_leverage,
            fee_bps=args.fee_bps,
            max_cycles=args.max_cycles,
        )


__all__ = ["LiveRunConfig"]
