from __future__ import annotations

SUPPORTED_EXECUTION_STRATEGIES = {"rp_daily_breakout"}
UNSUPPORTED_HTF_EXECUTION_OPTIONS = (
    "--disable-htf-filter",
    "--htf-interval",
    "--htf-entry-lookback",
    "--htf-expansion-bars",
    "--htf-expansion-min-growth",
    "--disable-htf-expansion-filter",
)

__all__ = ["SUPPORTED_EXECUTION_STRATEGIES", "UNSUPPORTED_HTF_EXECUTION_OPTIONS"]
