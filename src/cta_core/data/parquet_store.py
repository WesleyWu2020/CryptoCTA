from __future__ import annotations

from pathlib import Path

import polars as pl


def append_closed_bars(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = pl.read_parquet(path)
        combined = pl.concat([existing, df], how="vertical_relaxed")
        combined = combined.unique(subset=["symbol", "interval", "open_time"], keep="last")
    else:
        combined = df
    combined.write_parquet(path)
