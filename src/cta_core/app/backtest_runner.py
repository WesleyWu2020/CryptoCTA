from __future__ import annotations

import polars as pl

from cta_core.events.models import EventType


def run_backtest(*, bars: pl.DataFrame, symbol: str) -> dict[str, list[dict[str, object]]]:
    required_columns = ("open_time", "close")
    missing_columns = [column for column in required_columns if column not in bars.columns]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"bars missing required columns: {missing}")

    events: list[dict[str, object]] = []
    for row in bars.iter_rows(named=True):
        events.append(
            {
                "type": EventType.BAR_CLOSED.value,
                "symbol": symbol,
                "open_time": row["open_time"],
                "close": row["close"],
            }
        )
    return {"events": events}
