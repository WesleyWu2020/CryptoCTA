from __future__ import annotations

from cta_core.execution.live_binance import LiveBinanceAdapter
from cta_core.app.live_config import LiveRunConfig


def bootstrap_live_runner(api_key: str, api_secret: str) -> LiveBinanceAdapter:
    return LiveBinanceAdapter(api_key=api_key, api_secret=api_secret)


def main(argv: list[str] | None = None) -> int:
    config = LiveRunConfig.from_argv(argv)
    if config.dry_run:
        return 0

    bootstrap_live_runner(api_key=config.api_key, api_secret=config.api_secret)
    return 0


__all__ = ["bootstrap_live_runner", "main"]
