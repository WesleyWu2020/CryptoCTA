import math

import polars as pl
import pytest

from cta_core.app.turtle_backtest import run_turtle_backtest


def _build_daily_rp_signal_bars(step_ms: int = 86400000) -> pl.DataFrame:
    close = [100.0] * 20 + [103.0, 106.0, 109.0, 111.0] + [108.0, 104.0, 99.0, 95.0] + [95.0] * 8
    open_time = [i * step_ms for i in range(len(close))]
    return pl.DataFrame(
        {
            "open_time": open_time,
            "open": close,
            "high": [c + 2.0 for c in close],
            "low": [c - 2.0 for c in close],
            "close": close,
            "volume": [1000.0] * len(close),
        }
    )


def _build_daily_rp_short_signal_bars(step_ms: int = 86400000) -> pl.DataFrame:
    close = [100.0] * 20 + [97.0, 94.0, 91.0, 89.0] + [92.0, 96.0, 101.0, 106.0] + [106.0] * 8
    open_time = [i * step_ms for i in range(len(close))]
    return pl.DataFrame(
        {
            "open_time": open_time,
            "open": close,
            "high": [c + 2.0 for c in close],
            "low": [c - 2.0 for c in close],
            "close": close,
            "volume": [1000.0] * len(close),
        }
    )


def _rp_from_close_and_volume(close: list[float], volume: list[float], window: int, base_turnover: float, max_turnover_cap: float) -> list[float]:
    log_vol = [math.log(v + 1.0) for v in volume]
    turnover: list[float] = []
    for i in range(len(log_vol)):
        start = max(0, i - window + 1)
        segment = log_vol[start : i + 1]
        roll_min = min(segment)
        roll_max = max(segment)
        denom = roll_max - roll_min
        if denom == 0:
            denom = 1.0
        pos = (log_vol[i] - roll_min) / denom
        est = pos * max_turnover_cap
        t = base_turnover + est * (1.0 - base_turnover)
        turnover.append(min(max(t, 0.0), 0.99))

    rp = [close[0]]
    curr = close[0]
    for i in range(1, len(close)):
        curr = curr * (1.0 - turnover[i - 1]) + close[i - 1] * turnover[i - 1]
        rp.append(curr)
    return rp


def test_daily_rp_break_strategy_generates_entry_and_exit():
    bars = _build_daily_rp_signal_bars()

    out = run_turtle_backtest(
        bars=bars,
        symbol="BTCUSDT",
        interval="1d",
        use_htf_filter=False,
        use_regime_filter=False,
        use_rp_chop_filter=False,
        use_rp_signal_quality_sizing=False,
        rp_entry_confirm_bars=1,
        rp_exit_confirm_bars=1,
        cooldown_bars=0,
        allow_short=False,
    )

    actions = [t["action"] for t in out["trades"]]
    assert "ENTER_LONG_RP2" in actions
    assert "EXIT_LONG_RP2" in actions
    assert out["summary"]["closed_trades"] >= 1


def test_single_bar_breakout_uses_next_open_for_fill():
    bars = _build_daily_rp_signal_bars()
    close = bars.get_column("close").to_list()
    volume = bars.get_column("volume").to_list()

    out = run_turtle_backtest(
        bars=bars,
        symbol="BTCUSDT",
        interval="1d",
        use_htf_filter=False,
        use_regime_filter=False,
        use_rp_chop_filter=False,
        use_rp_signal_quality_sizing=False,
        rp_entry_confirm_bars=1,
        rp_exit_confirm_bars=1,
        cooldown_bars=0,
        allow_short=False,
    )

    rp = _rp_from_close_and_volume(close=close, volume=volume, window=100, base_turnover=0.02, max_turnover_cap=0.8)

    signal_idx = None
    for i in range(1, len(close)):
        if close[i] > rp[i]:
            signal_idx = i
            break
    assert signal_idx is not None

    entry = next(t for t in out["trades"] if t["action"] == "ENTER_LONG_RP2")
    assert entry["open_time"] == bars.get_column("open_time").to_list()[signal_idx + 1]


def test_signal_on_last_bar_does_not_trade_without_next_open():
    close = [100.0] * 10 + [130.0]
    bars = pl.DataFrame(
        {
            "open_time": [i * 86400000 for i in range(len(close))],
            "open": close,
            "high": [c + 1.0 for c in close],
            "low": [c - 1.0 for c in close],
            "close": close,
            "volume": [1000.0] * len(close),
        }
    )

    out = run_turtle_backtest(
        bars=bars,
        symbol="BTCUSDT",
        interval="1d",
        use_htf_filter=False,
        rp_entry_confirm_bars=1,
        rp_exit_confirm_bars=1,
        cooldown_bars=0,
        allow_short=False,
    )

    assert out["summary"]["total_trades"] == 0


def test_full_allocation_entry_uses_all_cash():
    bars = _build_daily_rp_signal_bars()
    out = run_turtle_backtest(
        bars=bars,
        symbol="BTCUSDT",
        interval="1d",
        use_htf_filter=False,
        use_rp_chop_filter=False,
        use_rp_signal_quality_sizing=False,
        rp_entry_confirm_bars=1,
        rp_exit_confirm_bars=1,
        cooldown_bars=0,
        allow_short=False,
        initial_capital=1000.0,
        fee_bps=0.0,
        slippage_bps=0.0,
        max_leverage=1.0,
    )

    entry = next(t for t in out["trades"] if t["action"] == "ENTER_LONG_RP2")
    assert entry["qty"] == pytest.approx(1000.0 / entry["price"], rel=1e-9)


def test_works_without_volume_column_with_safe_fallback():
    bars = _build_daily_rp_signal_bars().drop("volume")

    out = run_turtle_backtest(
        bars=bars,
        symbol="BTCUSDT",
        interval="1d",
        use_htf_filter=False,
        rp_entry_confirm_bars=1,
        rp_exit_confirm_bars=1,
        allow_short=False,
    )

    assert "summary" in out
    assert out["summary"]["total_trades"] >= 0


def test_requires_ohlc_columns():
    bars = pl.DataFrame({"open_time": [1, 2, 3], "close": [1.0, 2.0, 3.0]})

    with pytest.raises(ValueError, match="bars missing required columns"):
        run_turtle_backtest(
            bars=bars,
            symbol="BTCUSDT",
            interval="1d",
            use_htf_filter=False,
            allow_short=False,
        )


def test_regime_filter_can_block_entries_when_trend_requirement_too_strict():
    bars = _build_daily_rp_signal_bars()

    loose = run_turtle_backtest(
        bars=bars,
        symbol="BTCUSDT",
        interval="1d",
        use_htf_filter=False,
        rp_entry_confirm_bars=1,
        rp_exit_confirm_bars=1,
        allow_short=False,
    )
    assert loose["summary"]["total_trades"] > 0

    strict = run_turtle_backtest(
        bars=bars,
        symbol="BTCUSDT",
        interval="1d",
        use_htf_filter=False,
        rp_entry_confirm_bars=1,
        rp_exit_confirm_bars=1,
        allow_short=False,
        use_regime_filter=True,
        regime_ema_window=5,
        regime_min_slope=0.2,
    )
    assert strict["summary"]["total_trades"] == 0


def test_time_stop_exit_reason_is_emitted():
    bars = _build_daily_rp_signal_bars()
    out = run_turtle_backtest(
        bars=bars,
        symbol="BTCUSDT",
        interval="1d",
        use_htf_filter=False,
        rp_entry_confirm_bars=1,
        rp_exit_confirm_bars=1,
        allow_short=False,
        max_hold_bars=1,
    )

    exits = [t for t in out["trades"] if t["action"] == "EXIT_LONG_RP2"]
    assert exits
    assert any(t.get("exit_reason") == "TIME_STOP" for t in exits)


def test_vol_target_position_sizing_reduces_qty_in_high_vol_regime():
    low_vol_close = [100.0] * 20 + [101.0, 102.0, 103.0, 104.0, 105.0] + [104.0, 102.0, 100.0]
    high_vol_close = [100.0] * 10 + [80.0, 120.0] * 5 + [101.0, 103.0, 106.0, 109.0] + [107.0, 103.0, 99.0]

    def _build(close: list[float]) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "open_time": [i * 86400000 for i in range(len(close))],
                "open": close,
                "high": [c + 2.0 for c in close],
                "low": [c - 2.0 for c in close],
                "close": close,
                "volume": [1000.0] * len(close),
            }
        )

    low_out = run_turtle_backtest(
        bars=_build(low_vol_close),
        symbol="BTCUSDT",
        interval="1d",
        use_htf_filter=False,
        rp_entry_confirm_bars=1,
        rp_exit_confirm_bars=1,
        allow_short=False,
        use_vol_target_sizing=True,
        target_annual_vol=0.15,
        vol_target_window=5,
    )
    high_out = run_turtle_backtest(
        bars=_build(high_vol_close),
        symbol="BTCUSDT",
        interval="1d",
        use_htf_filter=False,
        rp_entry_confirm_bars=1,
        rp_exit_confirm_bars=1,
        allow_short=False,
        use_vol_target_sizing=True,
        target_annual_vol=0.15,
        vol_target_window=5,
    )

    low_entry = next(t for t in low_out["trades"] if t["action"] == "ENTER_LONG_RP2")
    high_entry = next(t for t in high_out["trades"] if t["action"] == "ENTER_LONG_RP2")
    assert low_entry["qty"] > high_entry["qty"]


def test_daily_rp_break_strategy_supports_short_entry_and_exit():
    bars = _build_daily_rp_short_signal_bars()

    out = run_turtle_backtest(
        bars=bars,
        symbol="BTCUSDT",
        interval="1d",
        use_htf_filter=False,
        use_regime_filter=False,
        rp_entry_confirm_bars=1,
        rp_exit_confirm_bars=1,
        cooldown_bars=0,
        allow_short=True,
    )

    actions = [t["action"] for t in out["trades"]]
    assert "ENTER_SHORT_RP2" in actions
    assert "EXIT_SHORT_RP2" in actions

    entry = next(t for t in out["trades"] if t["action"] == "ENTER_SHORT_RP2")
    exit_trade = next(t for t in out["trades"] if t["action"] == "EXIT_SHORT_RP2")
    assert entry["side"] == "SELL"
    assert exit_trade["side"] == "BUY"


def test_daily_rp_break_strategy_no_short_when_disabled():
    bars = _build_daily_rp_short_signal_bars()

    out = run_turtle_backtest(
        bars=bars,
        symbol="BTCUSDT",
        interval="1d",
        use_htf_filter=False,
        use_regime_filter=False,
        rp_entry_confirm_bars=1,
        rp_exit_confirm_bars=1,
        cooldown_bars=0,
        allow_short=False,
    )

    actions = [t["action"] for t in out["trades"]]
    assert "ENTER_SHORT_RP2" not in actions
    assert "EXIT_SHORT_RP2" not in actions


def test_rp_chop_filter_blocks_entries_when_slope_or_atr_ratio_too_strict():
    bars = _build_daily_rp_signal_bars()

    loose = run_turtle_backtest(
        bars=bars,
        symbol="BTCUSDT",
        interval="1d",
        use_htf_filter=False,
        use_regime_filter=False,
        rp_entry_confirm_bars=1,
        rp_exit_confirm_bars=1,
        cooldown_bars=0,
        allow_short=False,
        use_rp_chop_filter=True,
        rp_min_slope_ratio=0.0,
        rp_min_atr_ratio=0.0,
    )
    assert loose["summary"]["total_trades"] > 0

    strict = run_turtle_backtest(
        bars=bars,
        symbol="BTCUSDT",
        interval="1d",
        use_htf_filter=False,
        use_regime_filter=False,
        rp_entry_confirm_bars=1,
        rp_exit_confirm_bars=1,
        cooldown_bars=0,
        allow_short=False,
        use_rp_chop_filter=True,
        rp_min_slope_ratio=0.2,
        rp_min_atr_ratio=0.2,
    )
    assert strict["summary"]["total_trades"] == 0


def test_rp_signal_quality_sizing_increases_qty_for_stronger_breakout():
    def _build(close: list[float]) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "open_time": [i * 86400000 for i in range(len(close))],
                "open": close,
                "high": [c + 1.0 for c in close],
                "low": [c - 1.0 for c in close],
                "close": close,
                "volume": [1000.0] * len(close),
            }
        )

    low_signal_close = [100.0] * 20 + [101.0, 101.5, 102.0, 102.5] + [101.0, 99.0, 97.0] + [97.0] * 5
    high_signal_close = [100.0] * 20 + [108.0, 110.0, 112.0, 114.0] + [111.0, 106.0, 100.0] + [100.0] * 5

    low_out = run_turtle_backtest(
        bars=_build(low_signal_close),
        symbol="BTCUSDT",
        interval="1d",
        use_htf_filter=False,
        use_regime_filter=False,
        use_vol_target_sizing=False,
        rp_entry_confirm_bars=1,
        rp_exit_confirm_bars=1,
        cooldown_bars=0,
        allow_short=False,
        use_rp_signal_quality_sizing=True,
        rp_quality_target_atr=5.0,
        rp_quality_min_scale=0.1,
    )
    high_out = run_turtle_backtest(
        bars=_build(high_signal_close),
        symbol="BTCUSDT",
        interval="1d",
        use_htf_filter=False,
        use_regime_filter=False,
        use_vol_target_sizing=False,
        rp_entry_confirm_bars=1,
        rp_exit_confirm_bars=1,
        cooldown_bars=0,
        allow_short=False,
        use_rp_signal_quality_sizing=True,
        rp_quality_target_atr=5.0,
        rp_quality_min_scale=0.1,
    )

    low_entry = next(t for t in low_out["trades"] if t["action"] == "ENTER_LONG_RP2")
    high_entry = next(t for t in high_out["trades"] if t["action"] == "ENTER_LONG_RP2")
    assert high_entry["signal_quality_scale"] > low_entry["signal_quality_scale"]
    assert high_entry["vol_target_allocation"] > low_entry["vol_target_allocation"]
