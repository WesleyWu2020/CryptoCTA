import polars as pl
import pytest

from cta_core.data.data_portal import DataPortal, FutureDataAccessError


def test_data_portal_rejects_future_access():
    df = pl.DataFrame(
        {
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "interval": ["15m", "15m"],
            "open_time": [1000, 2000],
            "close": [10.0, 11.0],
        }
    )
    portal = DataPortal(df, latest_open_time=2000)
    with pytest.raises(FutureDataAccessError):
        portal.closed_bars("BTCUSDT", "15m", end_open_time=3000, lookback=2)
