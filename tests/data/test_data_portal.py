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


def test_data_portal_allows_latest_boundary_and_returns_expected_window():
    df = pl.DataFrame(
        {
            "symbol": ["BTCUSDT", "BTCUSDT", "BTCUSDT"],
            "interval": ["15m", "15m", "15m"],
            "open_time": [1000, 2000, 3000],
            "close": [10.0, 11.0, 12.0],
        }
    )
    portal = DataPortal(df, latest_open_time=2000)

    result = portal.closed_bars("BTCUSDT", "15m", end_open_time=2000, lookback=2)

    assert result.select("open_time").to_series().to_list() == [1000, 2000]
    assert result.select("close").to_series().to_list() == [10.0, 11.0]


@pytest.mark.parametrize("lookback", [0, -1])
def test_data_portal_rejects_non_positive_lookback(lookback: int):
    df = pl.DataFrame(
        {
            "symbol": ["BTCUSDT"],
            "interval": ["15m"],
            "open_time": [1000],
            "close": [10.0],
        }
    )
    portal = DataPortal(df, latest_open_time=1000)

    with pytest.raises(ValueError, match="lookback"):
        portal.closed_bars("BTCUSDT", "15m", end_open_time=1000, lookback=lookback)
