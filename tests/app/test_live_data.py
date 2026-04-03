import polars as pl

from cta_core.app import live_data


class FakeClient:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def fetch_klines(self, symbol, interval, limit, start_time=None, end_time=None):
        self.calls.append(
            {
                "symbol": symbol,
                "interval": interval,
                "limit": limit,
                "start_time": start_time,
                "end_time": end_time,
            }
        )
        return self.rows


def test_fetch_closed_bars_filters_out_unclosed_latest_bar() -> None:
    client = FakeClient(
        rows=[
            [1000, "10", "11", "9", "10.5", "1", 1999, "0", 1, "0", "0", "0"],
            [2000, "10.5", "12", "10", "11.5", "1", 2999, "0", 1, "0", "0", "0"],
            [3000, "11.5", "13", "11", "12.5", "1", 3999, "0", 1, "0", "0", "0"],
        ]
    )

    bars = live_data.fetch_closed_bars(
        client=client,
        symbol="BTCUSDT",
        interval="1m",
        lookback_bars=10,
        now_ms=3500,
    )

    assert client.calls == [
        {
            "symbol": "BTCUSDT",
            "interval": "1m",
            "limit": 12,
            "start_time": None,
            "end_time": None,
        }
    ]
    assert bars.select("open_time").to_series().to_list() == [1000, 2000]
    assert bars.height == 2
    assert "close" in bars.columns
    assert "close_time" in bars.columns


def test_select_new_closed_bars_returns_only_bars_after_checkpoint() -> None:
    bars = pl.DataFrame(
        {
            "open_time": [1000, 2000, 3000],
            "close": [10.0, 11.0, 12.0],
            "close_time": [1999, 2999, 3999],
        }
    )

    selected = live_data.select_new_closed_bars(bars, last_processed_open_time=2000)

    assert selected.select("open_time").to_series().to_list() == [3000]
