from __future__ import annotations

from cta_core.execution.live_binance import LiveBinanceAdapter


def bootstrap_live_runner(api_key: str, api_secret: str) -> LiveBinanceAdapter:
    return LiveBinanceAdapter(api_key=api_key, api_secret=api_secret)
