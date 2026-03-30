from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
import polars as pl

from cta_core.app.turtle_backtest import run_turtle_backtest
from cta_core.data.market_data_store import utc_ms


def _load_bars_from_duckdb(
    *,
    db_path: Path,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
) -> pl.DataFrame:
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        table = conn.execute(
            """
            SELECT open_time, open, high, low, close, volume
            FROM futures_klines
            WHERE symbol = ?
              AND interval = ?
              AND open_time >= ?
              AND open_time < ?
            ORDER BY open_time
            """,
            [symbol, interval, start_ms, end_ms],
        ).to_arrow_table()
    finally:
        conn.close()
    return pl.from_arrow(table)


def _base_config() -> dict[str, object]:
    # v3 relaxed baseline used in recent manual runs.
    return {
        "atr_lookback": 20,
        "initial_capital": 100000.0,
        "fee_bps": 5.0,
        "slippage_bps": 1.0,
        "stop_atr_multiple": 2.0,
        "max_leverage": 1.0,
        "allow_short": False,
        "trend_ema_window": 200,
        "cooldown_bars": 4,
        "entry_lookback": 55,
        "exit_lookback": 20,
        "risk_per_trade": 0.005,
        "pullback_window": 12,
        "pullback_tolerance_atr": 0.25,
        "breakout_entry_fraction": 0.35,
        "require_channel_expansion": True,
        "expansion_bars": 3,
        "expansion_min_growth": 1.05,
        "use_trend_strength_layering": True,
        "strong_trend_threshold": 6.0,
        "weak_exit_lookback": 20,
        "strong_exit_lookback": 25,
        "weak_pullback_tolerance_atr": 0.2,
        "strong_pullback_tolerance_atr": 0.4,
        "weak_trend_pullback_only": True,
        "weak_trend_no_trade": True,
        "add_on_only_after_profit": True,
        "addon_min_unrealized_r": 0.5,
        "min_breakout_distance_atr": 0.0,
        "min_breakout_body_atr": 0.0,
        "enable_partial_take_profit": True,
        "take_profit_r_multiple": 1.0,
        "take_profit_fraction": 0.5,
        "use_signal_score_filter": True,
        "min_signal_score_ratio": 0.3,
        "min_position_scale": 0.35,
        "follow_through_bars": 1,
        "follow_through_max_wait_bars": 3,
        "max_hold_bars": 96,
        "use_htf_filter": True,
        "htf_entry_lookback": 20,
        "htf_expansion_bars": 3,
        "htf_expansion_min_growth": 1.05,
        "htf_require_channel_expansion": True,
    }


def _build_scenarios() -> list[tuple[str, dict[str, object]]]:
    base = _base_config()
    return [
        ("baseline_v3_relaxed", base),
        ("ablate_weak_trend_no_trade", {**base, "weak_trend_no_trade": False}),
        ("ablate_follow_through_to_2", {**base, "follow_through_bars": 2}),
        ("ablate_signal_score_filter", {**base, "use_signal_score_filter": False}),
        ("ablate_addon_profit_gate", {**base, "add_on_only_after_profit": False}),
        ("ablate_partial_take_profit", {**base, "enable_partial_take_profit": False}),
        ("ablate_enable_shorts", {**base, "allow_short": True}),
    ]


def _run_case(
    *,
    name: str,
    params: dict[str, object],
    bars_main: pl.DataFrame,
    bars_htf: pl.DataFrame | None,
    symbol: str,
    main_interval: str,
) -> dict[str, object]:
    out = run_turtle_backtest(
        bars=bars_main,
        bars_htf=bars_htf,
        symbol=symbol,
        interval=main_interval,
        **params,
    )
    summary = out["summary"]
    start_open_time = out["start_open_time"]
    end_open_time = out["end_open_time"]
    if start_open_time is not None and end_open_time is not None and end_open_time > start_open_time:
        days = (end_open_time - start_open_time) / 1000.0 / 86400.0
    else:
        days = 0.0
    closed_trades = int(summary["closed_trades"])
    closed_per_30d = 0.0 if days <= 0 else closed_trades / (days / 30.0)
    return {
        "scenario": name,
        "return_pct": float(summary["return_pct"]),
        "net_pnl": float(summary["net_pnl"]),
        "max_drawdown": float(summary["max_drawdown"]),
        "total_trades": int(summary["total_trades"]),
        "closed_trades": closed_trades,
        "closed_trades_per_30d": closed_per_30d,
        "win_rate": float(summary["win_rate"]),
        "profit_factor": None if summary["profit_factor"] is None else float(summary["profit_factor"]),
        "breakout_entry_alpha_pnl": float(summary.get("breakout_entry_alpha_pnl", 0.0)),
        "pullback_entry_alpha_pnl": float(summary.get("pullback_entry_alpha_pnl", 0.0)),
        "exit_alpha_pnl": float(summary.get("exit_alpha_pnl", 0.0)),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Single-factor ablation for turtle strategy.")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--main-interval", default="1h")
    parser.add_argument("--htf-interval", default="1d")
    parser.add_argument("--start", default="2024-09-01")
    parser.add_argument("--end", default="2026-03-16", help="exclusive date")
    parser.add_argument("--db-path", type=Path, default=Path("artifacts/market_data/klines.duckdb"))
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts/ablation"))
    parser.add_argument("--base-name", default="btcusdt_1h_1d_rp2_single_factor_ablation")
    args = parser.parse_args()

    start_ms = utc_ms(args.start)
    end_ms = utc_ms(args.end)
    bars_main = _load_bars_from_duckdb(
        db_path=args.db_path,
        symbol=args.symbol,
        interval=args.main_interval,
        start_ms=start_ms,
        end_ms=end_ms,
    )
    bars_htf = _load_bars_from_duckdb(
        db_path=args.db_path,
        symbol=args.symbol,
        interval=args.htf_interval,
        start_ms=start_ms,
        end_ms=end_ms,
    )
    if bars_main.height == 0 or bars_htf.height == 0:
        raise ValueError("insufficient bars loaded for ablation")

    scenarios = _build_scenarios()
    rows: list[dict[str, object]] = []
    for name, params in scenarios:
        row = _run_case(
            name=name,
            params=params,
            bars_main=bars_main,
            bars_htf=bars_htf,
            symbol=args.symbol,
            main_interval=args.main_interval,
        )
        rows.append(row)

    baseline = rows[0]
    for row in rows:
        row["delta_return_pct_vs_base"] = float(row["return_pct"]) - float(baseline["return_pct"])
        row["delta_net_pnl_vs_base"] = float(row["net_pnl"]) - float(baseline["net_pnl"])
        row["delta_closed_trades_vs_base"] = int(row["closed_trades"]) - int(baseline["closed_trades"])

    out_dir = args.output_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"{args.base_name}.csv"
    json_path = out_dir / f"{args.base_name}.json"
    pl.DataFrame(rows).write_csv(csv_path)
    json_path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"csv={csv_path}")
    print(f"json={json_path}")
    print("rows:")
    for row in rows:
        print(row)


if __name__ == "__main__":
    main()
