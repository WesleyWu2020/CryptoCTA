from __future__ import annotations

import argparse
import json
from bisect import bisect_left
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

import duckdb
import polars as pl

from cta_core.app.turtle_backtest import run_turtle_backtest
from cta_core.data.market_data_store import utc_ms


@dataclass(frozen=True)
class Fold:
    fold_id: int
    train_start_ms: int
    train_end_ms: int
    test_start_ms: int
    test_end_ms: int


class BarsSlicer:
    def __init__(self, bars: pl.DataFrame):
        self._bars = bars.sort("open_time")
        self._open_times = self._bars.get_column("open_time").to_list()

    def slice(self, start_ms: int, end_ms: int) -> pl.DataFrame:
        left = bisect_left(self._open_times, start_ms)
        right = bisect_left(self._open_times, end_ms)
        return self._bars.slice(left, right - left)

    @property
    def height(self) -> int:
        return self._bars.height


def _parse_int_list(raw: str) -> list[int]:
    return [int(x.strip()) for x in raw.split(",") if x.strip()]


def _parse_float_list(raw: str) -> list[float]:
    return [float(x.strip()) for x in raw.split(",") if x.strip()]


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


def _build_folds(*, start_ms: int, end_ms: int, train_days: int, test_days: int, step_days: int) -> list[Fold]:
    folds: list[Fold] = []
    day_ms = 24 * 60 * 60 * 1000
    train_ms = train_days * day_ms
    test_ms = test_days * day_ms
    step_ms = step_days * day_ms

    fold_start = start_ms
    fold_id = 1
    while fold_start + train_ms + test_ms <= end_ms:
        train_start = fold_start
        train_end = fold_start + train_ms
        test_start = train_end
        test_end = train_end + test_ms
        folds.append(
            Fold(
                fold_id=fold_id,
                train_start_ms=train_start,
                train_end_ms=train_end,
                test_start_ms=test_start,
                test_end_ms=test_end,
            )
        )
        fold_start += step_ms
        fold_id += 1

    return folds


def _fmt_utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")


def _param_grid(args: argparse.Namespace) -> list[dict[str, float | int | bool | None]]:
    grid: list[dict[str, float | int | bool | None]] = []
    for entry in _parse_int_list(args.grid_entry_lookback):
        for weak_exit in _parse_int_list(args.grid_weak_exit_lookback):
            if weak_exit >= entry:
                continue
            for risk in _parse_float_list(args.grid_risk_per_trade):
                for pullback_w in _parse_int_list(args.grid_pullback_window):
                    for weak_pullback_tol in _parse_float_list(args.grid_weak_pullback_tolerance_atr):
                        for htf_growth in _parse_float_list(args.grid_htf_expansion_min_growth):
                            for breakout_fraction in _parse_float_list(args.grid_breakout_entry_fraction):
                                for strong_threshold in _parse_float_list(args.grid_strong_trend_threshold):
                                    for strong_exit in _parse_int_list(args.grid_strong_exit_lookback):
                                        if strong_exit < weak_exit:
                                            continue
                                        for strong_pullback_tol in _parse_float_list(args.grid_strong_pullback_tolerance_atr):
                                            if strong_pullback_tol < weak_pullback_tol:
                                                continue
                                            grid.append(
                                                {
                                                    "entry_lookback": entry,
                                                    "weak_exit_lookback": weak_exit,
                                                    "strong_exit_lookback": strong_exit,
                                                    "risk_per_trade": risk,
                                                    "pullback_window": pullback_w,
                                                    "weak_pullback_tolerance_atr": weak_pullback_tol,
                                                    "strong_pullback_tolerance_atr": strong_pullback_tol,
                                                    "breakout_entry_fraction": breakout_fraction,
                                                    "strong_trend_threshold": strong_threshold,
                                                    "htf_expansion_min_growth": htf_growth,
                                                }
                                            )
    return grid


def _run_single(
    *,
    bars_main: pl.DataFrame,
    bars_htf: pl.DataFrame | None,
    main_interval: str,
    symbol: str,
    params: dict[str, float | int | bool | None],
    fee_bps: float,
    slippage_bps: float,
    initial_capital: float,
) -> dict[str, float | int | None]:
    out = run_turtle_backtest(
        bars=bars_main,
        bars_htf=bars_htf,
        symbol=symbol,
        interval=main_interval,
        atr_lookback=20,
        initial_capital=initial_capital,
        fee_bps=fee_bps,
        slippage_bps=slippage_bps,
        stop_atr_multiple=2.0,
        max_leverage=1.0,
        allow_short=True,
        trend_ema_window=200,
        cooldown_bars=4,
        require_channel_expansion=True,
        expansion_bars=3,
        expansion_min_growth=1.05,
        use_htf_filter=True,
        htf_entry_lookback=20,
        htf_expansion_bars=3,
        htf_require_channel_expansion=True,
        entry_lookback=int(params["entry_lookback"]),
        exit_lookback=int(params["weak_exit_lookback"]),
        risk_per_trade=float(params["risk_per_trade"]),
        pullback_window=int(params["pullback_window"]),
        pullback_tolerance_atr=float(params["weak_pullback_tolerance_atr"]),
        breakout_entry_fraction=float(params["breakout_entry_fraction"]),
        use_trend_strength_layering=True,
        strong_trend_threshold=float(params["strong_trend_threshold"]),
        weak_exit_lookback=int(params["weak_exit_lookback"]),
        strong_exit_lookback=int(params["strong_exit_lookback"]),
        weak_pullback_tolerance_atr=float(params["weak_pullback_tolerance_atr"]),
        strong_pullback_tolerance_atr=float(params["strong_pullback_tolerance_atr"]),
        weak_trend_pullback_only=True,
        min_breakout_distance_atr=0.15,
        min_breakout_body_atr=0.25,
        enable_partial_take_profit=True,
        take_profit_r_multiple=1.0,
        take_profit_fraction=0.5,
        use_signal_score_filter=True,
        min_signal_score_ratio=0.6,
        min_position_scale=0.35,
        follow_through_bars=2,
        follow_through_max_wait_bars=3,
        max_hold_bars=96,
        rp_turnover_window=100,
        rp_base_turnover=0.02,
        rp_max_turnover_cap=0.8,
        rp_entry_confirm_bars=3,
        rp_exit_confirm_bars=3,
        rp_entry_band_atr=0.25,
        rp_exit_band_atr=0.25,
        rp_min_hold_bars=6,
        rp_htf_slope_bars=1,
        htf_expansion_min_growth=float(params["htf_expansion_min_growth"]),
    )
    s = out["summary"]
    return {
        "net_pnl": float(s["net_pnl"]),
        "return_pct": float(s["return_pct"]),
        "max_drawdown": float(s["max_drawdown"]),
        "profit_factor": None if s["profit_factor"] is None else float(s["profit_factor"]),
        "total_trades": int(s["total_trades"]),
        "closed_trades": int(s["closed_trades"]),
    }


def _aggregate(
    *,
    grid: list[dict[str, float | int | bool | None]],
    fold_rows: list[dict[str, object]],
) -> list[dict[str, object]]:
    by_idx: dict[int, list[dict[str, object]]] = {}
    for row in fold_rows:
        idx = int(row["param_id"])
        by_idx.setdefault(idx, []).append(row)

    summary_rows: list[dict[str, object]] = []
    for idx, params in enumerate(grid, start=1):
        rows = by_idx.get(idx, [])
        if not rows:
            continue

        test_returns = [float(r["test_return_pct"]) for r in rows]
        test_net_pnl = [float(r["test_net_pnl"]) for r in rows]
        test_drawdown = [float(r["test_max_drawdown"]) for r in rows]

        compounded = 1.0
        for r in test_returns:
            compounded *= 1.0 + r

        folds = len(rows)
        positive_folds = sum(1 for r in test_returns if r > 0)

        summary_rows.append(
            {
                "param_id": idx,
                **params,
                "folds": folds,
                "oos_total_return_pct": compounded - 1.0,
                "oos_sum_net_pnl": sum(test_net_pnl),
                "oos_avg_return_pct": sum(test_returns) / folds,
                "oos_median_return_pct": sorted(test_returns)[folds // 2] if folds % 2 == 1 else (sorted(test_returns)[folds // 2 - 1] + sorted(test_returns)[folds // 2]) / 2.0,
                "oos_positive_fold_ratio": positive_folds / folds,
                "oos_worst_drawdown": max(test_drawdown),
                "oos_avg_drawdown": sum(test_drawdown) / folds,
            }
        )

    summary_rows.sort(
        key=lambda x: (
            float(x["oos_total_return_pct"]),
            float(x["oos_avg_return_pct"]),
            -float(x["oos_worst_drawdown"]),
        ),
        reverse=True,
    )
    return summary_rows


def _write_outputs(
    *,
    output_dir: Path,
    base_name: str,
    fold_rows: list[dict[str, object]],
    summary_rows: list[dict[str, object]],
    top_k: int,
) -> tuple[Path, Path, Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)

    fold_df = pl.DataFrame(fold_rows)
    summary_df = pl.DataFrame(summary_rows)
    top_df = summary_df.head(top_k)

    fold_csv = output_dir / f"{base_name}_fold_details.csv"
    summary_csv = output_dir / f"{base_name}_summary.csv"
    top_csv = output_dir / f"{base_name}_top{top_k}.csv"
    top_json = output_dir / f"{base_name}_top{top_k}.json"

    fold_df.write_csv(fold_csv)
    summary_df.write_csv(summary_csv)
    top_df.write_csv(top_csv)
    top_json.write_text(json.dumps(top_df.to_dicts(), indent=2, ensure_ascii=False), encoding="utf-8")

    return fold_csv, summary_csv, top_csv, top_json


def main() -> None:
    parser = argparse.ArgumentParser(description="Walk-forward robustness test for MTF turtle strategy.")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--main-interval", default="1h")
    parser.add_argument("--htf-interval", default="1d")
    parser.add_argument("--start", default="2024-09-01")
    parser.add_argument("--end", default="2026-03-16", help="exclusive date")
    parser.add_argument("--db-path", type=Path, default=Path("artifacts/market_data/klines.duckdb"))
    parser.add_argument("--output-dir", type=Path, default=Path("artifacts/walkforward"))

    parser.add_argument("--train-days", type=int, default=180)
    parser.add_argument("--test-days", type=int, default=30)
    parser.add_argument("--step-days", type=int, default=30)
    parser.add_argument("--top-k", type=int, default=10)

    parser.add_argument("--initial-capital", type=float, default=100000.0)
    parser.add_argument("--fee-bps", type=float, default=5.0)
    parser.add_argument("--slippage-bps", type=float, default=1.0)

    parser.add_argument("--grid-entry-lookback", default="40,55,70")
    parser.add_argument("--grid-weak-exit-lookback", default="15,20")
    parser.add_argument("--grid-strong-exit-lookback", default="25")
    parser.add_argument("--grid-risk-per-trade", default="0.003,0.005")
    parser.add_argument("--grid-pullback-window", default="8,12")
    parser.add_argument("--grid-weak-pullback-tolerance-atr", default="0.2,0.3")
    parser.add_argument("--grid-strong-pullback-tolerance-atr", default="0.4")
    parser.add_argument("--grid-breakout-entry-fraction", default="0.35")
    parser.add_argument("--grid-strong-trend-threshold", default="6.0,8.0")
    parser.add_argument("--grid-htf-expansion-min-growth", default="1.03,1.05")

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
        raise ValueError("insufficient bars loaded for walk-forward")

    slicer_main = BarsSlicer(bars_main)
    slicer_htf = BarsSlicer(bars_htf)

    folds = _build_folds(
        start_ms=start_ms,
        end_ms=end_ms,
        train_days=args.train_days,
        test_days=args.test_days,
        step_days=args.step_days,
    )
    if not folds:
        raise ValueError("no folds generated; adjust date range or window sizes")

    grid = _param_grid(args)
    if not grid:
        raise ValueError("parameter grid is empty")

    fold_rows: list[dict[str, object]] = []

    total_runs = len(grid) * len(folds)
    run_idx = 0
    for param_id, params in enumerate(grid, start=1):
        for fold in folds:
            train_main = slicer_main.slice(fold.train_start_ms, fold.train_end_ms)
            train_htf = slicer_htf.slice(fold.train_start_ms, fold.train_end_ms)
            test_main = slicer_main.slice(fold.test_start_ms, fold.test_end_ms)
            test_htf = slicer_htf.slice(fold.test_start_ms, fold.test_end_ms)

            train_metrics = _run_single(
                bars_main=train_main,
                bars_htf=train_htf,
                main_interval=args.main_interval,
                symbol=args.symbol,
                params=params,
                fee_bps=args.fee_bps,
                slippage_bps=args.slippage_bps,
                initial_capital=args.initial_capital,
            )
            test_metrics = _run_single(
                bars_main=test_main,
                bars_htf=test_htf,
                main_interval=args.main_interval,
                symbol=args.symbol,
                params=params,
                fee_bps=args.fee_bps,
                slippage_bps=args.slippage_bps,
                initial_capital=args.initial_capital,
            )

            run_idx += 1
            if run_idx % 100 == 0 or run_idx == total_runs:
                print(f"progress {run_idx}/{total_runs}")

            fold_rows.append(
                {
                    "param_id": param_id,
                    **params,
                    "fold_id": fold.fold_id,
                    "train_start": _fmt_utc(fold.train_start_ms),
                    "train_end": _fmt_utc(fold.train_end_ms),
                    "test_start": _fmt_utc(fold.test_start_ms),
                    "test_end": _fmt_utc(fold.test_end_ms),
                    "train_net_pnl": train_metrics["net_pnl"],
                    "train_return_pct": train_metrics["return_pct"],
                    "train_max_drawdown": train_metrics["max_drawdown"],
                    "train_profit_factor": train_metrics["profit_factor"],
                    "train_total_trades": train_metrics["total_trades"],
                    "test_net_pnl": test_metrics["net_pnl"],
                    "test_return_pct": test_metrics["return_pct"],
                    "test_max_drawdown": test_metrics["max_drawdown"],
                    "test_profit_factor": test_metrics["profit_factor"],
                    "test_total_trades": test_metrics["total_trades"],
                }
            )

    summary_rows = _aggregate(grid=grid, fold_rows=fold_rows)

    base_name = (
        f"{args.symbol.lower()}_{args.main_interval}_{args.htf_interval}_"
        f"wf_{args.start}_{args.end}_train{args.train_days}_test{args.test_days}_step{args.step_days}"
    )
    fold_csv, summary_csv, top_csv, top_json = _write_outputs(
        output_dir=args.output_dir,
        base_name=base_name,
        fold_rows=fold_rows,
        summary_rows=summary_rows,
        top_k=args.top_k,
    )

    print(f"fold_csv={fold_csv}")
    print(f"summary_csv={summary_csv}")
    print(f"top_csv={top_csv}")
    print(f"top_json={top_json}")
    print("top_params:")
    for row in summary_rows[: args.top_k]:
        print(
            {
                "param_id": row["param_id"],
                "oos_total_return_pct": row["oos_total_return_pct"],
                "oos_sum_net_pnl": row["oos_sum_net_pnl"],
                "oos_worst_drawdown": row["oos_worst_drawdown"],
                "entry_lookback": row["entry_lookback"],
                "weak_exit_lookback": row["weak_exit_lookback"],
                "strong_exit_lookback": row["strong_exit_lookback"],
                "risk_per_trade": row["risk_per_trade"],
                "pullback_window": row["pullback_window"],
                "weak_pullback_tolerance_atr": row["weak_pullback_tolerance_atr"],
                "strong_pullback_tolerance_atr": row["strong_pullback_tolerance_atr"],
                "breakout_entry_fraction": row["breakout_entry_fraction"],
                "strong_trend_threshold": row["strong_trend_threshold"],
                "htf_expansion_min_growth": row["htf_expansion_min_growth"],
            }
        )


if __name__ == "__main__":
    main()
