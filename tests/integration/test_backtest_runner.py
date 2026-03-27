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
    assert len(output["events"]) == len(bars)
    assert [event["type"] for event in output["events"]] == ["BAR_CLOSED"] * len(bars)
    assert [event["symbol"] for event in output["events"]] == ["BTCUSDT"] * len(bars)
    assert [event["open_time"] for event in output["events"]] == bars["open_time"].to_list()
    assert [event["close"] for event in output["events"]] == bars["close"].to_list()


def test_backtest_requires_open_time_and_close_columns():
    bars = pl.DataFrame({"open_time": [1, 2, 3], "open": [10, 11, 12]})

    try:
        run_backtest(bars=bars, symbol="BTCUSDT")
    except ValueError as exc:
        assert str(exc) == "bars missing required columns: close"
    else:
        raise AssertionError("expected ValueError for missing close column")
