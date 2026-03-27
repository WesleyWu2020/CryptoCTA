from __future__ import annotations

import polars as pl


class FutureDataAccessError(ValueError):
    pass


class DataPortal:
    def __init__(self, bars: pl.DataFrame, latest_open_time: int):
        self._bars = bars
        self._latest_open_time = latest_open_time

    def closed_bars(
        self,
        symbol: str,
        interval: str,
        end_open_time: int,
        lookback: int,
    ) -> pl.DataFrame:
        if end_open_time > self._latest_open_time:
            raise FutureDataAccessError(
                f"requested={end_open_time}, latest={self._latest_open_time}"
            )
        if lookback <= 0:
            raise ValueError(f"lookback must be positive, got {lookback}")
        return (
            self._bars.filter(
                (pl.col("symbol") == symbol)
                & (pl.col("interval") == interval)
                & (pl.col("open_time") <= end_open_time)
            )
            .sort("open_time")
            .tail(lookback)
        )
