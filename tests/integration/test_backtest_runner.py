import polars as pl

from cta_core.app.backtest_runner import run_backtest


def test_backtest_returns_replayable_events():
    bars = pl.DataFrame(
        {
            "open_time": [1, 2, 3, 4, 5, 6],
            "open": [10, 11, 12, 13, 14, 15],
            "high": [11, 12, 13, 14, 15, 16],
            "low": [9, 10, 11, 12, 13, 14],
            "close": [10, 11, 12, 13, 14, 15],
        }
    )
    output = run_backtest(bars=bars, symbol="BTCUSDT")
    assert len(output["events"]) > 0
    assert output["events"][0]["type"] == "BAR_CLOSED"
