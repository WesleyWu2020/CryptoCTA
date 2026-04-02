from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any
import json
import math

import polars as pl

from cta_core.strategy_runtime.base import StrategyContext, StrategyDecisionType
from cta_core.strategy_runtime.strategies import RPDailyBreakoutConfig, RPDailyBreakoutStrategy


@dataclass(frozen=True)
class TurtleConfig:
    entry_lookback: int = 55
    exit_lookback: int = 20
    atr_lookback: int = 20
    initial_capital: float = 100000.0
    risk_per_trade: float = 0.005
    fee_bps: float = 5.0
    slippage_bps: float = 1.0
    stop_atr_multiple: float = 2.0
    max_leverage: float = 1.0
    allow_short: bool = False
    trend_ema_window: int | None = 200
    cooldown_bars: int = 4
    pullback_window: int = 12
    pullback_tolerance_atr: float = 0.25
    require_channel_expansion: bool = True
    expansion_bars: int = 3
    expansion_min_growth: float = 1.05
    breakout_entry_fraction: float = 0.35

    use_trend_strength_layering: bool = True
    strong_trend_threshold: float = 6.0
    weak_exit_lookback: int | None = None
    strong_exit_lookback: int | None = None
    weak_pullback_tolerance_atr: float | None = None
    strong_pullback_tolerance_atr: float | None = None
    weak_trend_pullback_only: bool = False
    weak_trend_no_trade: bool = False
    add_on_only_after_profit: bool = False
    addon_min_unrealized_r: float = 0.5

    min_breakout_distance_atr: float = 0.0
    min_breakout_body_atr: float = 0.0

    enable_partial_take_profit: bool = False
    take_profit_r_multiple: float = 1.0
    take_profit_fraction: float = 0.5

    use_signal_score_filter: bool = True
    min_signal_score_ratio: float = 0.6
    min_position_scale: float = 0.35

    follow_through_bars: int = 1
    follow_through_max_wait_bars: int = 3
    max_hold_bars: int | None = 40

    use_htf_filter: bool = True
    htf_entry_lookback: int = 20
    htf_expansion_bars: int = 3
    htf_expansion_min_growth: float = 1.05
    htf_require_channel_expansion: bool = True

    rp_turnover_window: int = 100
    rp_base_turnover: float = 0.02
    rp_max_turnover_cap: float = 0.8
    rp_window: int = 3
    rp_quantity: Decimal = Decimal("1")
    rp_entry_confirm_bars: int = 3
    rp_exit_confirm_bars: int = 3
    rp_entry_band_atr: float = 0.0
    rp_exit_band_atr: float = 0.0
    rp_min_hold_bars: int = 0
    rp_htf_slope_bars: int = 1
    use_rp_chop_filter: bool = False
    rp_slope_bars: int = 3
    rp_min_slope_ratio: float = 0.0005
    rp_min_atr_ratio: float = 0.008
    use_rp_signal_quality_sizing: bool = False
    rp_quality_target_atr: float = 1.0
    rp_quality_min_scale: float = 0.35

    use_regime_filter: bool = True
    regime_ema_window: int = 30
    regime_slope_bars: int = 3
    regime_min_slope: float = 0.002

    use_vol_target_sizing: bool = False
    target_annual_vol: float = 0.15
    vol_target_window: int = 20
    min_position_allocation: float = 0.2

    def validate(self) -> None:
        if self.entry_lookback <= 1:
            raise ValueError("entry_lookback must be > 1")
        if self.exit_lookback <= 1:
            raise ValueError("exit_lookback must be > 1")
        if self.atr_lookback <= 1:
            raise ValueError("atr_lookback must be > 1")
        if self.initial_capital <= 0:
            raise ValueError("initial_capital must be > 0")
        if not (0 < self.risk_per_trade < 1):
            raise ValueError("risk_per_trade must be between 0 and 1")
        if self.fee_bps < 0:
            raise ValueError("fee_bps must be >= 0")
        if self.slippage_bps < 0:
            raise ValueError("slippage_bps must be >= 0")
        if self.stop_atr_multiple <= 0:
            raise ValueError("stop_atr_multiple must be > 0")
        if self.max_leverage <= 0:
            raise ValueError("max_leverage must be > 0")
        if self.cooldown_bars < 0:
            raise ValueError("cooldown_bars must be >= 0")
        if self.pullback_window < 0:
            raise ValueError("pullback_window must be >= 0")
        if self.pullback_tolerance_atr < 0:
            raise ValueError("pullback_tolerance_atr must be >= 0")
        if self.expansion_bars < 2:
            raise ValueError("expansion_bars must be >= 2")
        if self.expansion_min_growth <= 0:
            raise ValueError("expansion_min_growth must be > 0")
        if not (0 < self.breakout_entry_fraction < 1):
            raise ValueError("breakout_entry_fraction must be between 0 and 1")
        if self.strong_trend_threshold <= 0:
            raise ValueError("strong_trend_threshold must be > 0")
        if self.weak_exit_lookback is not None and self.weak_exit_lookback <= 1:
            raise ValueError("weak_exit_lookback must be > 1 when provided")
        if self.strong_exit_lookback is not None and self.strong_exit_lookback <= 1:
            raise ValueError("strong_exit_lookback must be > 1 when provided")
        if self.weak_pullback_tolerance_atr is not None and self.weak_pullback_tolerance_atr < 0:
            raise ValueError("weak_pullback_tolerance_atr must be >= 0 when provided")
        if self.strong_pullback_tolerance_atr is not None and self.strong_pullback_tolerance_atr < 0:
            raise ValueError("strong_pullback_tolerance_atr must be >= 0 when provided")
        if self.addon_min_unrealized_r < 0:
            raise ValueError("addon_min_unrealized_r must be >= 0")
        if self.min_breakout_distance_atr < 0:
            raise ValueError("min_breakout_distance_atr must be >= 0")
        if self.min_breakout_body_atr < 0:
            raise ValueError("min_breakout_body_atr must be >= 0")
        if self.take_profit_r_multiple <= 0:
            raise ValueError("take_profit_r_multiple must be > 0")
        if not (0 < self.take_profit_fraction < 1):
            raise ValueError("take_profit_fraction must be between 0 and 1")
        if not (0 < self.min_signal_score_ratio <= 1):
            raise ValueError("min_signal_score_ratio must be in (0, 1]")
        if not (0 < self.min_position_scale <= 1):
            raise ValueError("min_position_scale must be in (0, 1]")
        if self.follow_through_bars < 1:
            raise ValueError("follow_through_bars must be >= 1")
        if self.follow_through_max_wait_bars < self.follow_through_bars:
            raise ValueError("follow_through_max_wait_bars must be >= follow_through_bars")
        if self.max_hold_bars is not None and self.max_hold_bars < 1:
            raise ValueError("max_hold_bars must be >= 1 when provided")
        if self.htf_entry_lookback <= 1:
            raise ValueError("htf_entry_lookback must be > 1")
        if self.htf_expansion_bars < 2:
            raise ValueError("htf_expansion_bars must be >= 2")
        if self.htf_expansion_min_growth <= 0:
            raise ValueError("htf_expansion_min_growth must be > 0")
        if self.trend_ema_window is not None and self.trend_ema_window <= 1:
            raise ValueError("trend_ema_window must be > 1 when provided")
        if self.rp_turnover_window <= 1:
            raise ValueError("rp_turnover_window must be > 1")
        if not (0 <= self.rp_base_turnover < 1):
            raise ValueError("rp_base_turnover must be in [0, 1)")
        if not (0 < self.rp_max_turnover_cap <= 1):
            raise ValueError("rp_max_turnover_cap must be in (0, 1]")
        if self.rp_window <= 0:
            raise ValueError("rp_window must be > 0")
        if self.rp_quantity <= 0:
            raise ValueError("rp_quantity must be > 0")
        if self.rp_entry_confirm_bars < 1:
            raise ValueError("rp_entry_confirm_bars must be >= 1")
        if self.rp_exit_confirm_bars < 1:
            raise ValueError("rp_exit_confirm_bars must be >= 1")
        if self.rp_entry_band_atr < 0:
            raise ValueError("rp_entry_band_atr must be >= 0")
        if self.rp_exit_band_atr < 0:
            raise ValueError("rp_exit_band_atr must be >= 0")
        if self.rp_min_hold_bars < 0:
            raise ValueError("rp_min_hold_bars must be >= 0")
        if self.rp_htf_slope_bars < 1:
            raise ValueError("rp_htf_slope_bars must be >= 1")
        if self.rp_slope_bars < 1:
            raise ValueError("rp_slope_bars must be >= 1")
        if self.rp_min_slope_ratio < 0:
            raise ValueError("rp_min_slope_ratio must be >= 0")
        if self.rp_min_atr_ratio < 0:
            raise ValueError("rp_min_atr_ratio must be >= 0")
        if self.rp_quality_target_atr <= 0:
            raise ValueError("rp_quality_target_atr must be > 0")
        if not (0 < self.rp_quality_min_scale <= 1):
            raise ValueError("rp_quality_min_scale must be in (0, 1]")
        if self.regime_ema_window <= 1:
            raise ValueError("regime_ema_window must be > 1")
        if self.regime_slope_bars < 1:
            raise ValueError("regime_slope_bars must be >= 1")
        if self.target_annual_vol <= 0:
            raise ValueError("target_annual_vol must be > 0")
        if self.vol_target_window <= 1:
            raise ValueError("vol_target_window must be > 1")
        if not (0 < self.min_position_allocation <= 1):
            raise ValueError("min_position_allocation must be in (0, 1]")

    @classmethod
    def from_flat_kwargs(cls, **kwargs: Any) -> TurtleConfig:
        field_names = {f.name for f in dataclasses.fields(cls)}
        filtered = {k: v for k, v in kwargs.items() if k in field_names}
        return cls(**filtered)


def run_turtle_backtest(
    *,
    bars: pl.DataFrame,
    symbol: str,
    interval: str,
    config: TurtleConfig | None = None,
    bars_htf: pl.DataFrame | None = None,
    **kwargs: Any,
) -> dict[str, object]:
    if config is None:
        config = TurtleConfig.from_flat_kwargs(**kwargs)
    cfg = config
    cfg.validate()

    if _can_use_runtime_rp_compat(cfg):
        return _run_rp_runtime_compat(
            bars=bars,
            symbol=symbol,
            interval=interval,
            config=cfg,
        )

    return _run_reference_price_strategy(
        bars=bars,
        bars_htf=bars_htf,
        symbol=symbol,
        interval=interval,
        config=cfg,
    )

    required_columns = ("open_time", "open", "high", "low", "close")
    missing_columns = [c for c in required_columns if c not in bars.columns]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"bars missing required columns: {missing}")

    frame = bars.select(*required_columns).sort("open_time")

    open_time = [int(v) for v in frame.get_column("open_time").to_list()]
    open_price = [float(v) for v in frame.get_column("open").to_list()]
    high = [float(v) for v in frame.get_column("high").to_list()]
    low = [float(v) for v in frame.get_column("low").to_list()]
    close = [float(v) for v in frame.get_column("close").to_list()]

    if not open_time:
        return _empty_result(symbol=symbol, interval=interval, config=cfg)

    main_step_ms = _infer_step_ms(open_time)

    if cfg.use_htf_filter:
        if bars_htf is None:
            raise ValueError("bars_htf is required when use_htf_filter=True")
        htf_regime = _build_htf_regime_for_main_bars(
            main_open_time=open_time,
            main_step_ms=main_step_ms,
            bars_htf=bars_htf,
            lookback=cfg.htf_entry_lookback,
            expansion_bars=cfg.htf_expansion_bars,
            expansion_min_growth=cfg.htf_expansion_min_growth,
            require_channel_expansion=cfg.htf_require_channel_expansion,
        )
    else:
        htf_regime = [
            {
                "long": True,
                "short": True,
                "htf_idx": None,
                "htf_open_time": None,
            }
            for _ in open_time
        ]

    if cfg.trend_ema_window is not None:
        if bars_htf is None:
            raise ValueError("bars_htf is required when trend_ema_window is set")
        htf_ema = _build_htf_ema_for_main_bars(
            main_open_time=open_time,
            main_step_ms=main_step_ms,
            bars_htf=bars_htf,
            ema_window=cfg.trend_ema_window,
        )
    else:
        htf_ema = [None for _ in open_time]

    tr = _true_range(high=high, low=low, close=close)
    atr = _rolling_mean(values=tr, window=cfg.atr_lookback)
    channel_width = _rolling_channel_width(high=high, low=low, window=cfg.entry_lookback)

    fee_rate = cfg.fee_bps / 10000.0
    slippage_rate = cfg.slippage_bps / 10000.0
    weak_exit_default = cfg.exit_lookback if cfg.weak_exit_lookback is None else cfg.weak_exit_lookback
    strong_exit_default = cfg.exit_lookback if cfg.strong_exit_lookback is None else cfg.strong_exit_lookback
    warmup = max(cfg.entry_lookback, cfg.atr_lookback, weak_exit_default, strong_exit_default)

    cash = cfg.initial_capital
    position_qty = 0.0
    stop_price: float | None = None
    last_exit_bar = -10**9

    entry_side: str | None = None
    position_avg_price = 0.0
    position_entry_fee = 0.0
    active_exit_lookback = cfg.exit_lookback
    active_pullback_tolerance_atr = cfg.pullback_tolerance_atr
    active_trend_bucket = "weak"
    initial_stop_distance = 0.0
    take_profit_done = False
    entry_bar_index: int | None = None

    pending_addon: dict[str, object] | None = None
    pending_pullback_entry: dict[str, object] | None = None
    pending_breakout_confirm: dict[str, object] | None = None

    open_legs: list[dict[str, float | str]] = []
    breakout_entry_alpha_pnl = 0.0
    pullback_entry_alpha_pnl = 0.0
    exit_alpha_pnl = 0.0

    realized_pnl = 0.0
    trades: list[dict[str, object]] = []
    equity_curve: list[dict[str, object]] = []
    closed_trade_pnls: list[float] = []

    max_equity = cfg.initial_capital
    max_drawdown = 0.0

    equity_curve.append(
        {
            "open_time": open_time[0],
            "close": close[0],
            "equity": cash,
            "position_qty": position_qty,
            "stop_price": stop_price,
            "realized_pnl": realized_pnl,
        }
    )

    for i in range(1, len(open_time)):
        prev = i - 1
        prev_close = close[prev]
        prev_equity = cash + position_qty * prev_close

        can_trade = prev >= warmup
        atr_prev = atr[prev]
        ema_prev = htf_ema[prev]

        if can_trade and atr_prev is not None:
            entry_high = max(high[prev - cfg.entry_lookback : prev])
            entry_low = min(low[prev - cfg.entry_lookback : prev])

            if cfg.enable_partial_take_profit and not take_profit_done and initial_stop_distance > 0 and position_qty > 0:
                target = position_avg_price + cfg.take_profit_r_multiple * initial_stop_distance
                if high[prev] >= target:
                    old_qty = abs(position_qty)
                    qty = old_qty * cfg.take_profit_fraction
                    if 0 < qty < old_qty:
                        fill = _fill_price(side="SELL", base_price=open_price[i], slippage_rate=slippage_rate)
                        fee = qty * fill * fee_rate
                        trade_pnl, by_source, consumed_entry_fee = _realize_from_legs(
                            legs=open_legs,
                            close_qty=qty,
                            close_price=fill,
                            close_fee=fee,
                            side="LONG",
                        )

                        cash += qty * fill - fee
                        realized_pnl += trade_pnl
                        position_qty = old_qty - qty
                        position_entry_fee = max(0.0, position_entry_fee - consumed_entry_fee)
                        take_profit_done = True
                        breakout_entry_alpha_pnl += by_source.get("breakout", 0.0)
                        pullback_entry_alpha_pnl += by_source.get("pullback", 0.0)
                        exit_alpha_pnl += trade_pnl

                        trades.append(
                            {
                                "open_time": open_time[i],
                                "side": "SELL",
                                "action": "TAKE_PROFIT_LONG",
                                "price": fill,
                                "qty": qty,
                                "fee": fee,
                                "trade_pnl": trade_pnl,
                                "target_price": target,
                                "trend_bucket": active_trend_bucket,
                                "effective_exit_lookback": active_exit_lookback,
                                "equity_after": cash + position_qty * close[i],
                            }
                        )

            if cfg.enable_partial_take_profit and not take_profit_done and initial_stop_distance > 0 and position_qty < 0:
                target = position_avg_price - cfg.take_profit_r_multiple * initial_stop_distance
                if low[prev] <= target:
                    old_qty = abs(position_qty)
                    qty = old_qty * cfg.take_profit_fraction
                    if 0 < qty < old_qty:
                        fill = _fill_price(side="BUY", base_price=open_price[i], slippage_rate=slippage_rate)
                        fee = qty * fill * fee_rate
                        trade_pnl, by_source, consumed_entry_fee = _realize_from_legs(
                            legs=open_legs,
                            close_qty=qty,
                            close_price=fill,
                            close_fee=fee,
                            side="SHORT",
                        )

                        cash -= qty * fill + fee
                        realized_pnl += trade_pnl
                        position_qty = -(old_qty - qty)
                        position_entry_fee = max(0.0, position_entry_fee - consumed_entry_fee)
                        take_profit_done = True
                        breakout_entry_alpha_pnl += by_source.get("breakout", 0.0)
                        pullback_entry_alpha_pnl += by_source.get("pullback", 0.0)
                        exit_alpha_pnl += trade_pnl

                        trades.append(
                            {
                                "open_time": open_time[i],
                                "side": "BUY",
                                "action": "TAKE_PROFIT_SHORT",
                                "price": fill,
                                "qty": qty,
                                "fee": fee,
                                "trade_pnl": trade_pnl,
                                "target_price": target,
                                "trend_bucket": active_trend_bucket,
                                "effective_exit_lookback": active_exit_lookback,
                                "equity_after": cash + position_qty * close[i],
                            }
                        )

            if position_qty > 0:
                should_exit = stop_price is not None and low[prev] <= stop_price
                if prev >= active_exit_lookback:
                    exit_low = min(low[prev - active_exit_lookback : prev])
                    should_exit = should_exit or prev_close < exit_low
                time_stop = False
                if cfg.max_hold_bars is not None and entry_bar_index is not None:
                    time_stop = (prev - entry_bar_index) >= cfg.max_hold_bars
                should_exit = should_exit or time_stop

                if should_exit:
                    fill = _fill_price(side="SELL", base_price=open_price[i], slippage_rate=slippage_rate)
                    qty = abs(position_qty)
                    fee = qty * fill * fee_rate
                    cash += qty * fill - fee

                    trade_pnl, by_source, consumed_entry_fee = _realize_from_legs(
                        legs=open_legs,
                        close_qty=qty,
                        close_price=fill,
                        close_fee=fee,
                        side="LONG",
                    )
                    realized_pnl += trade_pnl
                    closed_trade_pnls.append(trade_pnl)
                    breakout_entry_alpha_pnl += by_source.get("breakout", 0.0)
                    pullback_entry_alpha_pnl += by_source.get("pullback", 0.0)
                    if time_stop:
                        exit_alpha_pnl += trade_pnl
                    trades.append(
                        {
                            "open_time": open_time[i],
                            "side": "SELL",
                            "action": "EXIT_LONG",
                            "price": fill,
                            "qty": qty,
                            "fee": fee,
                            "trade_pnl": trade_pnl,
                            "trend_bucket": active_trend_bucket,
                            "effective_exit_lookback": active_exit_lookback,
                            "exit_reason": "TIME_STOP" if time_stop else "RULE_EXIT",
                            "equity_after": cash,
                        }
                    )

                    position_qty = 0.0
                    stop_price = None
                    entry_side = None
                    position_avg_price = 0.0
                    position_entry_fee = max(0.0, position_entry_fee - consumed_entry_fee)
                    pending_addon = None
                    pending_pullback_entry = None
                    pending_breakout_confirm = None
                    initial_stop_distance = 0.0
                    take_profit_done = False
                    entry_bar_index = None
                    open_legs = []
                    last_exit_bar = i

            elif position_qty < 0:
                qty = abs(position_qty)
                should_exit = stop_price is not None and high[prev] >= stop_price
                if prev >= active_exit_lookback:
                    exit_high = max(high[prev - active_exit_lookback : prev])
                    should_exit = should_exit or prev_close > exit_high
                time_stop = False
                if cfg.max_hold_bars is not None and entry_bar_index is not None:
                    time_stop = (prev - entry_bar_index) >= cfg.max_hold_bars
                should_exit = should_exit or time_stop

                if should_exit:
                    fill = _fill_price(side="BUY", base_price=open_price[i], slippage_rate=slippage_rate)
                    fee = qty * fill * fee_rate
                    cash -= qty * fill + fee

                    trade_pnl, by_source, consumed_entry_fee = _realize_from_legs(
                        legs=open_legs,
                        close_qty=qty,
                        close_price=fill,
                        close_fee=fee,
                        side="SHORT",
                    )
                    realized_pnl += trade_pnl
                    closed_trade_pnls.append(trade_pnl)
                    breakout_entry_alpha_pnl += by_source.get("breakout", 0.0)
                    pullback_entry_alpha_pnl += by_source.get("pullback", 0.0)
                    if time_stop:
                        exit_alpha_pnl += trade_pnl
                    trades.append(
                        {
                            "open_time": open_time[i],
                            "side": "BUY",
                            "action": "EXIT_SHORT",
                            "price": fill,
                            "qty": qty,
                            "fee": fee,
                            "trade_pnl": trade_pnl,
                            "trend_bucket": active_trend_bucket,
                            "effective_exit_lookback": active_exit_lookback,
                            "exit_reason": "TIME_STOP" if time_stop else "RULE_EXIT",
                            "equity_after": cash,
                        }
                    )

                    position_qty = 0.0
                    stop_price = None
                    entry_side = None
                    position_avg_price = 0.0
                    position_entry_fee = max(0.0, position_entry_fee - consumed_entry_fee)
                    pending_addon = None
                    pending_pullback_entry = None
                    pending_breakout_confirm = None
                    initial_stop_distance = 0.0
                    take_profit_done = False
                    entry_bar_index = None
                    open_legs = []
                    last_exit_bar = i

            if position_qty != 0.0 and pending_addon is not None:
                if prev > int(pending_addon["expiry_bar"]):
                    pending_addon = None
                else:
                    direction = str(pending_addon["direction"])
                    level = float(pending_addon["level"])
                    touched = bool(pending_addon["touched"])
                    breakout_atr = float(pending_addon["atr"])
                    tol_atr = float(pending_addon["effective_pullback_tolerance_atr"])
                    planned_qty = float(pending_addon["planned_qty"])
                    tol = tol_atr * breakout_atr

                    if direction == "LONG":
                        if cfg.add_on_only_after_profit and initial_stop_distance > 0:
                            profit_gate = prev_close >= position_avg_price + cfg.addon_min_unrealized_r * initial_stop_distance
                            if not profit_gate:
                                continue
                        if not touched and low[prev] <= level + tol:
                            touched = True
                        pending_addon["touched"] = touched

                        if touched and prev_close > level and planned_qty > 0:
                            max_qty = _position_size(
                                equity=prev_equity,
                                atr_value=float(atr_prev),
                                risk_per_trade=cfg.risk_per_trade,
                                execution_price=open_price[i],
                                max_leverage=cfg.max_leverage,
                                fee_rate=fee_rate,
                            )
                            qty = min(planned_qty, max_qty)
                            if qty > 0:
                                fill = _fill_price(side="BUY", base_price=open_price[i], slippage_rate=slippage_rate)
                                fee = qty * fill * fee_rate
                                cash -= qty * fill + fee

                                old_qty = abs(position_qty)
                                new_qty = old_qty + qty
                                position_avg_price = (position_avg_price * old_qty + fill * qty) / new_qty
                                position_entry_fee += fee
                                position_qty = new_qty
                                stop_price = position_avg_price - cfg.stop_atr_multiple * float(atr_prev)
                                entry_side = "LONG"
                                open_legs.append({"source": "pullback", "qty": qty, "entry_price": fill, "entry_fee": fee})

                                trades.append(
                                    {
                                        "open_time": open_time[i],
                                        "side": "BUY",
                                        "action": "ADD_LONG_PULLBACK",
                                        "price": fill,
                                        "qty": qty,
                                        "fee": fee,
                                        "atr": float(atr_prev),
                                        "pullback_level": level,
                                        "stop_price": stop_price,
                                        "trend_bucket": active_trend_bucket,
                                        "effective_exit_lookback": active_exit_lookback,
                                        "effective_pullback_tolerance_atr": active_pullback_tolerance_atr,
                                        "htf_open_time": pending_addon.get("htf_open_time"),
                                        "equity_after": cash + position_qty * close[i],
                                    }
                                )
                                pending_addon = None

                    elif direction == "SHORT":
                        if cfg.add_on_only_after_profit and initial_stop_distance > 0:
                            profit_gate = prev_close <= position_avg_price - cfg.addon_min_unrealized_r * initial_stop_distance
                            if not profit_gate:
                                continue
                        if not touched and high[prev] >= level - tol:
                            touched = True
                        pending_addon["touched"] = touched

                        if touched and prev_close < level and planned_qty > 0:
                            max_qty = _position_size(
                                equity=prev_equity,
                                atr_value=float(atr_prev),
                                risk_per_trade=cfg.risk_per_trade,
                                execution_price=open_price[i],
                                max_leverage=cfg.max_leverage,
                                fee_rate=fee_rate,
                            )
                            qty = min(planned_qty, max_qty)
                            if qty > 0:
                                fill = _fill_price(side="SELL", base_price=open_price[i], slippage_rate=slippage_rate)
                                fee = qty * fill * fee_rate
                                cash += qty * fill - fee

                                old_qty = abs(position_qty)
                                new_qty = old_qty + qty
                                position_avg_price = (position_avg_price * old_qty + fill * qty) / new_qty
                                position_entry_fee += fee
                                position_qty = -new_qty
                                stop_price = position_avg_price + cfg.stop_atr_multiple * float(atr_prev)
                                entry_side = "SHORT"
                                open_legs.append({"source": "pullback", "qty": qty, "entry_price": fill, "entry_fee": fee})

                                trades.append(
                                    {
                                        "open_time": open_time[i],
                                        "side": "SELL",
                                        "action": "ADD_SHORT_PULLBACK",
                                        "price": fill,
                                        "qty": qty,
                                        "fee": fee,
                                        "atr": float(atr_prev),
                                        "pullback_level": level,
                                        "stop_price": stop_price,
                                        "trend_bucket": active_trend_bucket,
                                        "effective_exit_lookback": active_exit_lookback,
                                        "effective_pullback_tolerance_atr": active_pullback_tolerance_atr,
                                        "htf_open_time": pending_addon.get("htf_open_time"),
                                        "equity_after": cash + position_qty * close[i],
                                    }
                                )
                                pending_addon = None

            if position_qty == 0.0 and prev > last_exit_bar + cfg.cooldown_bars:
                if pending_pullback_entry is not None:
                    if prev > int(pending_pullback_entry["expiry_bar"]):
                        pending_pullback_entry = None
                    else:
                        direction = str(pending_pullback_entry["direction"])
                        level = float(pending_pullback_entry["level"])
                        touched = bool(pending_pullback_entry["touched"])
                        breakout_atr = float(pending_pullback_entry["atr"])
                        tol_atr = float(pending_pullback_entry["effective_pullback_tolerance_atr"])
                        planned_qty = float(pending_pullback_entry["planned_qty"])
                        tol = tol_atr * breakout_atr

                        if direction == "LONG":
                            if not touched and low[prev] <= level + tol:
                                touched = True
                            pending_pullback_entry["touched"] = touched
                            if touched and prev_close > level and planned_qty > 0:
                                qty = min(
                                    planned_qty,
                                    _position_size(
                                        equity=prev_equity,
                                        atr_value=float(atr_prev),
                                        risk_per_trade=cfg.risk_per_trade,
                                        execution_price=open_price[i],
                                        max_leverage=cfg.max_leverage,
                                        fee_rate=fee_rate,
                                    ),
                                )
                                if qty > 0:
                                    fill = _fill_price(side="BUY", base_price=open_price[i], slippage_rate=slippage_rate)
                                    fee = qty * fill * fee_rate
                                    cash -= qty * fill + fee

                                    position_qty = qty
                                    position_avg_price = fill
                                    position_entry_fee = fee
                                    entry_side = "LONG"
                                    active_exit_lookback = int(pending_pullback_entry["effective_exit_lookback"])
                                    active_pullback_tolerance_atr = float(pending_pullback_entry["effective_pullback_tolerance_atr"])
                                    active_trend_bucket = str(pending_pullback_entry["trend_bucket"])
                                    stop_price = fill - cfg.stop_atr_multiple * float(atr_prev)
                                    initial_stop_distance = cfg.stop_atr_multiple * float(atr_prev)
                                    take_profit_done = False
                                    entry_bar_index = i
                                    open_legs.append({"source": "pullback", "qty": qty, "entry_price": fill, "entry_fee": fee})

                                    trades.append(
                                        {
                                            "open_time": open_time[i],
                                            "side": "BUY",
                                            "action": "ENTER_LONG_PULLBACK",
                                            "price": fill,
                                            "qty": qty,
                                            "fee": fee,
                                            "atr": float(atr_prev),
                                            "pullback_level": level,
                                            "stop_price": stop_price,
                                            "trend_bucket": active_trend_bucket,
                                            "effective_exit_lookback": active_exit_lookback,
                                            "effective_pullback_tolerance_atr": active_pullback_tolerance_atr,
                                            "htf_open_time": pending_pullback_entry.get("htf_open_time"),
                                            "equity_after": cash + position_qty * close[i],
                                        }
                                    )
                                    pending_pullback_entry = None

                        elif direction == "SHORT":
                            if not touched and high[prev] >= level - tol:
                                touched = True
                            pending_pullback_entry["touched"] = touched
                            if touched and prev_close < level and planned_qty > 0:
                                qty = min(
                                    planned_qty,
                                    _position_size(
                                        equity=prev_equity,
                                        atr_value=float(atr_prev),
                                        risk_per_trade=cfg.risk_per_trade,
                                        execution_price=open_price[i],
                                        max_leverage=cfg.max_leverage,
                                        fee_rate=fee_rate,
                                    ),
                                )
                                if qty > 0:
                                    fill = _fill_price(side="SELL", base_price=open_price[i], slippage_rate=slippage_rate)
                                    fee = qty * fill * fee_rate
                                    cash += qty * fill - fee

                                    position_qty = -qty
                                    position_avg_price = fill
                                    position_entry_fee = fee
                                    entry_side = "SHORT"
                                    active_exit_lookback = int(pending_pullback_entry["effective_exit_lookback"])
                                    active_pullback_tolerance_atr = float(pending_pullback_entry["effective_pullback_tolerance_atr"])
                                    active_trend_bucket = str(pending_pullback_entry["trend_bucket"])
                                    stop_price = fill + cfg.stop_atr_multiple * float(atr_prev)
                                    initial_stop_distance = cfg.stop_atr_multiple * float(atr_prev)
                                    take_profit_done = False
                                    entry_bar_index = i
                                    open_legs.append({"source": "pullback", "qty": qty, "entry_price": fill, "entry_fee": fee})

                                    trades.append(
                                        {
                                            "open_time": open_time[i],
                                            "side": "SELL",
                                            "action": "ENTER_SHORT_PULLBACK",
                                            "price": fill,
                                            "qty": qty,
                                            "fee": fee,
                                            "atr": float(atr_prev),
                                            "pullback_level": level,
                                            "stop_price": stop_price,
                                            "trend_bucket": active_trend_bucket,
                                            "effective_exit_lookback": active_exit_lookback,
                                            "effective_pullback_tolerance_atr": active_pullback_tolerance_atr,
                                            "htf_open_time": pending_pullback_entry.get("htf_open_time"),
                                            "equity_after": cash + position_qty * close[i],
                                        }
                                    )
                                    pending_pullback_entry = None

                if position_qty == 0.0 and pending_pullback_entry is None:
                    long_price_break = prev_close > entry_high
                    short_price_break = cfg.allow_short and prev_close < entry_low
                    long_ema_ok = ema_prev is None or prev_close > ema_prev
                    short_ema_ok = ema_prev is None or prev_close < ema_prev

                    expansion_ok = True
                    if cfg.require_channel_expansion:
                        expansion_ok = _channel_expansion_ok(
                            widths=channel_width,
                            idx=prev,
                            bars=cfg.expansion_bars,
                            min_growth=cfg.expansion_min_growth,
                        )

                    regime = htf_regime[prev]
                    long_regime_ok = bool(regime["long"])
                    short_regime_ok = bool(regime["short"])

                    body_abs = abs(prev_close - open_price[prev])
                    long_strength_ok = (
                        (prev_close - entry_high) >= cfg.min_breakout_distance_atr * float(atr_prev)
                        and body_abs >= cfg.min_breakout_body_atr * float(atr_prev)
                    )
                    short_strength_ok = (
                        (entry_low - prev_close) >= cfg.min_breakout_distance_atr * float(atr_prev)
                        and body_abs >= cfg.min_breakout_body_atr * float(atr_prev)
                    )

                    long_follow_ok = _follow_through_ok(
                        close=close,
                        idx=prev,
                        level=entry_high,
                        bars=cfg.follow_through_bars,
                        direction="LONG",
                    )
                    short_follow_ok = _follow_through_ok(
                        close=close,
                        idx=prev,
                        level=entry_low,
                        bars=cfg.follow_through_bars,
                        direction="SHORT",
                    )

                    long_break = long_price_break and long_ema_ok and long_regime_ok and expansion_ok and long_strength_ok and long_follow_ok
                    short_break = short_price_break and short_ema_ok and short_regime_ok and expansion_ok and short_strength_ok and short_follow_ok

                    if long_break or short_break:
                        trend_bucket, effective_exit_lookback, effective_pullback_tol = _effective_trend_params(
                            cfg=cfg,
                            width=channel_width[prev],
                            atr_value=float(atr_prev),
                        )
                        if cfg.weak_trend_no_trade and trend_bucket == "weak":
                            continue
                        long_score_ratio = _signal_score_ratio(
                            cfg=cfg,
                            trend_bucket=trend_bucket,
                            ema_ok=long_ema_ok,
                            regime_ok=long_regime_ok,
                            expansion_ok=expansion_ok,
                            strength_ok=long_strength_ok,
                            follow_ok=long_follow_ok,
                        )
                        short_score_ratio = _signal_score_ratio(
                            cfg=cfg,
                            trend_bucket=trend_bucket,
                            ema_ok=short_ema_ok,
                            regime_ok=short_regime_ok,
                            expansion_ok=expansion_ok,
                            strength_ok=short_strength_ok,
                            follow_ok=short_follow_ok,
                        )
                        if cfg.use_signal_score_filter:
                            long_break = long_break and long_score_ratio >= cfg.min_signal_score_ratio
                            short_break = short_break and short_score_ratio >= cfg.min_signal_score_ratio
                        if not (long_break or short_break):
                            continue

                        total_qty = _position_size(
                            equity=prev_equity,
                            atr_value=float(atr_prev),
                            risk_per_trade=cfg.risk_per_trade,
                            execution_price=open_price[i],
                            max_leverage=cfg.max_leverage,
                            fee_rate=fee_rate,
                        )
                        score_ratio = long_score_ratio if long_break else short_score_ratio
                        total_qty *= max(cfg.min_position_scale, min(score_ratio, 1.0))

                        if total_qty > 0 and trend_bucket == "weak" and cfg.weak_trend_pullback_only:
                            if long_break:
                                pending_pullback_entry = {
                                    "direction": "LONG",
                                    "level": entry_high,
                                    "atr": float(atr_prev),
                                    "touched": False,
                                    "planned_qty": total_qty,
                                    "expiry_bar": prev + cfg.pullback_window,
                                    "trend_bucket": trend_bucket,
                                    "effective_exit_lookback": effective_exit_lookback,
                                    "effective_pullback_tolerance_atr": effective_pullback_tol,
                                    "htf_open_time": regime.get("htf_open_time"),
                                }
                            elif short_break:
                                pending_pullback_entry = {
                                    "direction": "SHORT",
                                    "level": entry_low,
                                    "atr": float(atr_prev),
                                    "touched": False,
                                    "planned_qty": total_qty,
                                    "expiry_bar": prev + cfg.pullback_window,
                                    "trend_bucket": trend_bucket,
                                    "effective_exit_lookback": effective_exit_lookback,
                                    "effective_pullback_tolerance_atr": effective_pullback_tol,
                                    "htf_open_time": regime.get("htf_open_time"),
                                }

                        else:
                            breakout_qty = total_qty * cfg.breakout_entry_fraction
                            addon_qty = max(total_qty - breakout_qty, 0.0)
                            if breakout_qty > 0:
                                if long_break:
                                    fill = _fill_price(side="BUY", base_price=open_price[i], slippage_rate=slippage_rate)
                                    fee = breakout_qty * fill * fee_rate
                                    cash -= breakout_qty * fill + fee

                                    position_qty = breakout_qty
                                    position_avg_price = fill
                                    position_entry_fee = fee
                                    entry_side = "LONG"
                                    active_exit_lookback = effective_exit_lookback
                                    active_pullback_tolerance_atr = effective_pullback_tol
                                    active_trend_bucket = trend_bucket
                                    stop_price = fill - cfg.stop_atr_multiple * float(atr_prev)
                                    initial_stop_distance = cfg.stop_atr_multiple * float(atr_prev)
                                    take_profit_done = False
                                    pending_pullback_entry = None
                                    entry_bar_index = i
                                    open_legs.append({"source": "breakout", "qty": breakout_qty, "entry_price": fill, "entry_fee": fee})

                                    pending_addon = (
                                        {
                                            "direction": "LONG",
                                            "level": entry_high,
                                            "atr": float(atr_prev),
                                            "touched": False,
                                            "planned_qty": addon_qty,
                                            "expiry_bar": prev + cfg.pullback_window,
                                            "effective_pullback_tolerance_atr": effective_pullback_tol,
                                            "htf_open_time": regime.get("htf_open_time"),
                                        }
                                        if addon_qty > 0
                                        else None
                                    )

                                    trades.append(
                                        {
                                            "open_time": open_time[i],
                                            "side": "BUY",
                                            "action": "ENTER_LONG_BREAKOUT",
                                            "price": fill,
                                            "qty": breakout_qty,
                                            "fee": fee,
                                            "atr": float(atr_prev),
                                            "breakout_level": entry_high,
                                            "stop_price": stop_price,
                                            "trend_bucket": trend_bucket,
                                            "effective_exit_lookback": effective_exit_lookback,
                                            "effective_pullback_tolerance_atr": effective_pullback_tol,
                                            "htf_open_time": regime.get("htf_open_time"),
                                            "equity_after": cash + position_qty * close[i],
                                        }
                                    )
                                elif short_break:
                                    fill = _fill_price(side="SELL", base_price=open_price[i], slippage_rate=slippage_rate)
                                    fee = breakout_qty * fill * fee_rate
                                    cash += breakout_qty * fill - fee

                                    position_qty = -breakout_qty
                                    position_avg_price = fill
                                    position_entry_fee = fee
                                    entry_side = "SHORT"
                                    active_exit_lookback = effective_exit_lookback
                                    active_pullback_tolerance_atr = effective_pullback_tol
                                    active_trend_bucket = trend_bucket
                                    stop_price = fill + cfg.stop_atr_multiple * float(atr_prev)
                                    initial_stop_distance = cfg.stop_atr_multiple * float(atr_prev)
                                    take_profit_done = False
                                    pending_pullback_entry = None
                                    entry_bar_index = i
                                    open_legs.append({"source": "breakout", "qty": breakout_qty, "entry_price": fill, "entry_fee": fee})

                                    pending_addon = (
                                        {
                                            "direction": "SHORT",
                                            "level": entry_low,
                                            "atr": float(atr_prev),
                                            "touched": False,
                                            "planned_qty": addon_qty,
                                            "expiry_bar": prev + cfg.pullback_window,
                                            "effective_pullback_tolerance_atr": effective_pullback_tol,
                                            "htf_open_time": regime.get("htf_open_time"),
                                        }
                                        if addon_qty > 0
                                        else None
                                    )

                                    trades.append(
                                        {
                                            "open_time": open_time[i],
                                            "side": "SELL",
                                            "action": "ENTER_SHORT_BREAKOUT",
                                            "price": fill,
                                            "qty": breakout_qty,
                                            "fee": fee,
                                            "atr": float(atr_prev),
                                            "breakout_level": entry_low,
                                            "stop_price": stop_price,
                                            "trend_bucket": trend_bucket,
                                            "effective_exit_lookback": effective_exit_lookback,
                                            "effective_pullback_tolerance_atr": effective_pullback_tol,
                                            "htf_open_time": regime.get("htf_open_time"),
                                            "equity_after": cash + position_qty * close[i],
                                        }
                                    )

        equity = cash + position_qty * close[i]
        max_equity = max(max_equity, equity)
        drawdown = 0.0 if max_equity <= 0 else (max_equity - equity) / max_equity
        max_drawdown = max(max_drawdown, drawdown)

        equity_curve.append(
            {
                "open_time": open_time[i],
                "close": close[i],
                "equity": equity,
                "position_qty": position_qty,
                "stop_price": stop_price,
                "realized_pnl": realized_pnl,
            }
        )

    final_equity = cash + position_qty * close[-1]
    net_pnl = final_equity - cfg.initial_capital
    wins = sum(1 for pnl in closed_trade_pnls if pnl > 0)
    closed_trades = len(closed_trade_pnls)
    gross_profit = sum(pnl for pnl in closed_trade_pnls if pnl > 0)
    gross_loss = abs(sum(pnl for pnl in closed_trade_pnls if pnl < 0))
    profit_factor = None if gross_loss == 0 else gross_profit / gross_loss

    return {
        "symbol": symbol,
        "interval": interval,
        "start_open_time": open_time[0],
        "end_open_time": open_time[-1],
        "config": _config_dict(cfg),
        "summary": {
            "initial_capital": cfg.initial_capital,
            "final_equity": final_equity,
            "net_pnl": net_pnl,
            "return_pct": 0.0 if cfg.initial_capital == 0 else net_pnl / cfg.initial_capital,
            "max_drawdown": max_drawdown,
            "total_trades": len(trades),
            "closed_trades": closed_trades,
            "win_rate": 0.0 if closed_trades == 0 else wins / closed_trades,
            "gross_profit": gross_profit,
            "gross_loss": gross_loss,
            "profit_factor": profit_factor,
            "realized_pnl": realized_pnl,
            "breakout_entry_alpha_pnl": breakout_entry_alpha_pnl,
            "pullback_entry_alpha_pnl": pullback_entry_alpha_pnl,
            "exit_alpha_pnl": exit_alpha_pnl,
            "final_position_qty": position_qty,
            "open_position_side": entry_side,
        },
        "trades": trades,
        "equity_curve": equity_curve,
    }


def _run_reference_price_strategy(
    *,
    bars: pl.DataFrame,
    bars_htf: pl.DataFrame | None,
    symbol: str,
    interval: str,
    config: TurtleConfig,
) -> dict[str, object]:
    required_columns = ("open_time", "open", "high", "low", "close")
    missing_columns = [c for c in required_columns if c not in bars.columns]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"bars missing required columns: {missing}")

    frame = bars.sort("open_time")

    open_time = [int(v) for v in frame.get_column("open_time").to_list()]
    open_price = [float(v) for v in frame.get_column("open").to_list()]
    high = [float(v) for v in frame.get_column("high").to_list()]
    low = [float(v) for v in frame.get_column("low").to_list()]
    close = [float(v) for v in frame.get_column("close").to_list()]

    if not open_time:
        return _empty_result(symbol=symbol, interval=interval, config=config)

    if "volume" in frame.columns:
        volume = [max(float(v), 0.0) for v in frame.get_column("volume").to_list()]
    else:
        volume = [1.0 for _ in open_time]

    if "quote_volume" in frame.columns:
        quote_volume = [float(v) for v in frame.get_column("quote_volume").to_list()]
    elif "quote_asset_volume" in frame.columns:
        quote_volume = [float(v) for v in frame.get_column("quote_asset_volume").to_list()]
    else:
        quote_volume = [float("nan") for _ in open_time]

    vwap: list[float] = []
    for c, v, q in zip(close, volume, quote_volume):
        if v > 0 and q == q:
            vwap.append(q / v)
        else:
            vwap.append(c)

    turnover = _turnover_from_volume(
        volume=volume,
        window=config.rp_turnover_window,
        base_turnover=config.rp_base_turnover,
        max_turnover_cap=config.rp_max_turnover_cap,
    )
    reference_price = _reference_price_recursive(vwap=vwap, turnover=turnover)
    atr = _rolling_mean(values=_true_range(high=high, low=low, close=close), window=config.atr_lookback)
    regime_ema = _ema(values=close, window=config.regime_ema_window if config.use_regime_filter else None)
    log_returns = _log_returns(close=close)
    realized_vol = _rolling_std(values=log_returns, window=config.vol_target_window)

    fee_rate = config.fee_bps / 10000.0
    slippage_rate = config.slippage_bps / 10000.0
    warmup = max(
        1,
        config.rp_entry_confirm_bars,
        config.atr_lookback if (config.use_rp_chop_filter or config.use_rp_signal_quality_sizing) else 1,
        config.rp_slope_bars if config.use_rp_chop_filter else 1,
    )

    cash = config.initial_capital
    position_qty = 0.0
    position_avg_price = 0.0
    position_entry_fee = 0.0
    entry_bar_index: int | None = None
    last_exit_bar = -10**9

    realized_pnl = 0.0
    trades: list[dict[str, object]] = []
    equity_curve: list[dict[str, object]] = []
    closed_trade_pnls: list[float] = []

    max_equity = config.initial_capital
    max_drawdown = 0.0

    equity_curve.append(
        {
            "open_time": open_time[0],
            "close": close[0],
            "equity": cash,
            "position_qty": position_qty,
            "stop_price": None,
            "realized_pnl": realized_pnl,
        }
    )

    for i in range(1, len(open_time)):
        prev = i - 1
        prev_equity = cash + position_qty * close[prev]
        regime_long_ok = True
        regime_short_ok = True
        regime_slope = 0.0
        rp_slope_ratio = 0.0
        atr_ratio = 0.0
        atr_prev = atr[prev]
        if prev >= config.rp_slope_bars and reference_price[prev - config.rp_slope_bars] != 0:
            rp_slope_ratio = (
                reference_price[prev] - reference_price[prev - config.rp_slope_bars]
            ) / abs(reference_price[prev - config.rp_slope_bars])
        if atr_prev is not None and close[prev] > 0:
            atr_ratio = float(atr_prev) / close[prev]

        chop_long_ok = True
        chop_short_ok = True
        if config.use_rp_chop_filter:
            chop_long_ok = (
                prev >= config.rp_slope_bars
                and atr_prev is not None
                and rp_slope_ratio >= config.rp_min_slope_ratio
                and atr_ratio >= config.rp_min_atr_ratio
            )
            chop_short_ok = (
                prev >= config.rp_slope_bars
                and atr_prev is not None
                and rp_slope_ratio <= -config.rp_min_slope_ratio
                and atr_ratio >= config.rp_min_atr_ratio
            )

        regime_ema_prev = None if regime_ema is None else regime_ema[prev]
        if config.use_regime_filter:
            if regime_ema_prev is None or prev < config.regime_slope_bars:
                regime_long_ok = False
                regime_short_ok = False
            else:
                ema_past = regime_ema[prev - config.regime_slope_bars]
                if ema_past is None or ema_past == 0:
                    regime_long_ok = False
                    regime_short_ok = False
                else:
                    regime_slope = (float(regime_ema_prev) - float(ema_past)) / abs(float(ema_past))
                    regime_long_ok = close[prev] > float(regime_ema_prev) and regime_slope >= config.regime_min_slope
                    regime_short_ok = close[prev] < float(regime_ema_prev) and regime_slope <= -config.regime_min_slope

        can_check_entry_common = (
            prev >= warmup
            and prev >= config.rp_entry_confirm_bars - 1
            and (i - last_exit_bar) > config.cooldown_bars
        )

        if position_qty == 0.0 and can_check_entry_common:
            entered = False
            if regime_long_ok and chop_long_ok:
                entry_confirm = True
                for k in range(config.rp_entry_confirm_bars):
                    idx = prev - k
                    if close[idx] <= reference_price[idx]:
                        entry_confirm = False
                        break
                if entry_confirm:
                    fill = _fill_price(side="BUY", base_price=open_price[i], slippage_rate=slippage_rate)
                    allocation = config.max_leverage
                    if config.use_vol_target_sizing:
                        vol_prev = realized_vol[prev]
                        if vol_prev is not None and vol_prev > 0:
                            annual_vol = float(vol_prev) * math.sqrt(365.0)
                            if annual_vol > 0:
                                allocation = min(config.max_leverage, config.target_annual_vol / annual_vol)
                        allocation = max(config.min_position_allocation, allocation)
                    signal_quality_scale = 1.0
                    if config.use_rp_signal_quality_sizing:
                        strength = 0.0
                        if atr_prev is not None and float(atr_prev) > 0:
                            strength = max(0.0, (close[prev] - reference_price[prev]) / float(atr_prev))
                        signal_quality_scale = min(1.0, strength / config.rp_quality_target_atr)
                        signal_quality_scale = max(config.rp_quality_min_scale, signal_quality_scale)
                        allocation *= signal_quality_scale

                    qty = prev_equity * allocation / (fill * (1.0 + fee_rate)) if fill > 0 else 0.0
                    if qty > 0:
                        fee = qty * fill * fee_rate
                        cash -= qty * fill + fee
                        position_qty = qty
                        position_avg_price = fill
                        position_entry_fee = fee
                        entry_bar_index = i
                        entered = True

                        trades.append(
                            {
                                "open_time": open_time[i],
                                "side": "BUY",
                                "action": "ENTER_LONG_RP2",
                                "price": fill,
                                "qty": qty,
                                "fee": fee,
                                "rp_value": reference_price[prev],
                                "turnover_prev": turnover[prev],
                                "regime_slope": regime_slope,
                                "rp_slope_ratio": rp_slope_ratio,
                                "atr_ratio": atr_ratio,
                                "signal_quality_scale": signal_quality_scale,
                                "vol_target_allocation": allocation,
                                "equity_after": cash + position_qty * close[i],
                            }
                        )

            if (not entered) and config.allow_short and regime_short_ok and chop_short_ok:
                entry_confirm = True
                for k in range(config.rp_entry_confirm_bars):
                    idx = prev - k
                    if close[idx] >= reference_price[idx]:
                        entry_confirm = False
                        break
                if entry_confirm:
                    fill = _fill_price(side="SELL", base_price=open_price[i], slippage_rate=slippage_rate)
                    allocation = config.max_leverage
                    if config.use_vol_target_sizing:
                        vol_prev = realized_vol[prev]
                        if vol_prev is not None and vol_prev > 0:
                            annual_vol = float(vol_prev) * math.sqrt(365.0)
                            if annual_vol > 0:
                                allocation = min(config.max_leverage, config.target_annual_vol / annual_vol)
                        allocation = max(config.min_position_allocation, allocation)
                    signal_quality_scale = 1.0
                    if config.use_rp_signal_quality_sizing:
                        strength = 0.0
                        if atr_prev is not None and float(atr_prev) > 0:
                            strength = max(0.0, (reference_price[prev] - close[prev]) / float(atr_prev))
                        signal_quality_scale = min(1.0, strength / config.rp_quality_target_atr)
                        signal_quality_scale = max(config.rp_quality_min_scale, signal_quality_scale)
                        allocation *= signal_quality_scale

                    qty = prev_equity * allocation / (fill * (1.0 + fee_rate)) if fill > 0 else 0.0
                    if qty > 0:
                        fee = qty * fill * fee_rate
                        cash += qty * fill - fee
                        position_qty = -qty
                        position_avg_price = fill
                        position_entry_fee = fee
                        entry_bar_index = i

                        trades.append(
                            {
                                "open_time": open_time[i],
                                "side": "SELL",
                                "action": "ENTER_SHORT_RP2",
                                "price": fill,
                                "qty": qty,
                                "fee": fee,
                                "rp_value": reference_price[prev],
                                "turnover_prev": turnover[prev],
                                "regime_slope": regime_slope,
                                "rp_slope_ratio": rp_slope_ratio,
                                "atr_ratio": atr_ratio,
                                "signal_quality_scale": signal_quality_scale,
                                "vol_target_allocation": allocation,
                                "equity_after": cash + position_qty * close[i],
                            }
                        )

        elif position_qty > 0.0 and prev >= config.rp_exit_confirm_bars - 1:
            exit_confirm = True
            for k in range(config.rp_exit_confirm_bars):
                idx = prev - k
                if close[idx] >= reference_price[idx]:
                    exit_confirm = False
                    break
            hold_ok = entry_bar_index is None or (i - entry_bar_index) >= max(config.rp_min_hold_bars, 0)
            time_stop = False
            if config.max_hold_bars is not None and entry_bar_index is not None:
                time_stop = (i - entry_bar_index) >= config.max_hold_bars
            if (exit_confirm and hold_ok) or time_stop:
                fill = _fill_price(side="SELL", base_price=open_price[i], slippage_rate=slippage_rate)
                qty = abs(position_qty)
                fee = qty * fill * fee_rate
                cash += qty * fill - fee

                trade_pnl = (fill - position_avg_price) * qty - position_entry_fee - fee
                realized_pnl += trade_pnl
                closed_trade_pnls.append(trade_pnl)

                trades.append(
                    {
                        "open_time": open_time[i],
                        "side": "SELL",
                        "action": "EXIT_LONG_RP2",
                        "price": fill,
                        "qty": qty,
                        "fee": fee,
                        "trade_pnl": trade_pnl,
                        "rp_value": reference_price[prev],
                        "turnover_prev": turnover[prev],
                        "hold_bars": None if entry_bar_index is None else i - entry_bar_index,
                        "exit_reason": "TIME_STOP" if time_stop else "RP_CROSS",
                        "equity_after": cash,
                    }
                )

                position_qty = 0.0
                position_avg_price = 0.0
                position_entry_fee = 0.0
                entry_bar_index = None
                last_exit_bar = i

        elif position_qty < 0.0 and prev >= config.rp_exit_confirm_bars - 1:
            exit_confirm = True
            for k in range(config.rp_exit_confirm_bars):
                idx = prev - k
                if close[idx] <= reference_price[idx]:
                    exit_confirm = False
                    break
            hold_ok = entry_bar_index is None or (i - entry_bar_index) >= max(config.rp_min_hold_bars, 0)
            time_stop = False
            if config.max_hold_bars is not None and entry_bar_index is not None:
                time_stop = (i - entry_bar_index) >= config.max_hold_bars
            if (exit_confirm and hold_ok) or time_stop:
                fill = _fill_price(side="BUY", base_price=open_price[i], slippage_rate=slippage_rate)
                qty = abs(position_qty)
                fee = qty * fill * fee_rate
                cash -= qty * fill + fee

                trade_pnl = (position_avg_price - fill) * qty - position_entry_fee - fee
                realized_pnl += trade_pnl
                closed_trade_pnls.append(trade_pnl)

                trades.append(
                    {
                        "open_time": open_time[i],
                        "side": "BUY",
                        "action": "EXIT_SHORT_RP2",
                        "price": fill,
                        "qty": qty,
                        "fee": fee,
                        "trade_pnl": trade_pnl,
                        "rp_value": reference_price[prev],
                        "turnover_prev": turnover[prev],
                        "hold_bars": None if entry_bar_index is None else i - entry_bar_index,
                        "exit_reason": "TIME_STOP" if time_stop else "RP_CROSS",
                        "equity_after": cash,
                    }
                )

                position_qty = 0.0
                position_avg_price = 0.0
                position_entry_fee = 0.0
                entry_bar_index = None
                last_exit_bar = i

        mark_price = close[i]
        equity = cash + position_qty * mark_price
        max_equity = max(max_equity, equity)
        if max_equity > 0:
            max_drawdown = max(max_drawdown, (max_equity - equity) / max_equity)

        equity_curve.append(
            {
                "open_time": open_time[i],
                "close": mark_price,
                "equity": equity,
                "position_qty": position_qty,
                "stop_price": None,
                "realized_pnl": realized_pnl,
            }
        )

    final_equity = cash + position_qty * close[-1]
    net_pnl = final_equity - config.initial_capital
    wins = sum(1 for pnl in closed_trade_pnls if pnl > 0)
    closed_trades = len(closed_trade_pnls)
    gross_profit = sum(pnl for pnl in closed_trade_pnls if pnl > 0)
    gross_loss = abs(sum(pnl for pnl in closed_trade_pnls if pnl < 0))
    profit_factor = None if gross_loss == 0 else gross_profit / gross_loss
    win_rate = 0.0 if closed_trades == 0 else wins / closed_trades

    return {
        "symbol": symbol,
        "interval": interval,
        "start_open_time": open_time[0],
        "end_open_time": open_time[-1],
        "config": _config_dict(config),
        "summary": {
            "initial_capital": config.initial_capital,
            "final_equity": final_equity,
            "net_pnl": net_pnl,
            "return_pct": 0.0 if config.initial_capital == 0 else net_pnl / config.initial_capital,
            "max_drawdown": max_drawdown,
            "total_trades": len(trades),
            "closed_trades": closed_trades,
            "win_rate": win_rate,
            "gross_profit": gross_profit,
            "gross_loss": gross_loss,
            "profit_factor": profit_factor,
            "realized_pnl": realized_pnl,
            "breakout_entry_alpha_pnl": 0.0,
            "pullback_entry_alpha_pnl": 0.0,
            "exit_alpha_pnl": 0.0,
            "final_position_qty": position_qty,
            "open_position_side": "LONG" if position_qty > 0 else None,
        },
        "trades": trades,
        "equity_curve": equity_curve,
    }


def _can_use_runtime_rp_compat(config: TurtleConfig) -> bool:
    return (
        not config.allow_short
        and not config.use_htf_filter
        and not config.use_rp_chop_filter
        and not config.use_rp_signal_quality_sizing
        and not config.use_vol_target_sizing
        and not config.use_regime_filter
    )


def _run_rp_runtime_compat(
    *,
    bars: pl.DataFrame,
    symbol: str,
    interval: str,
    config: TurtleConfig,
) -> dict[str, object]:
    required_columns = ("open_time", "open", "high", "low", "close")
    missing_columns = [c for c in required_columns if c not in bars.columns]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"bars missing required columns: {missing}")

    frame = bars.sort("open_time")

    open_time = [int(v) for v in frame.get_column("open_time").to_list()]
    open_price = [float(v) for v in frame.get_column("open").to_list()]
    high = [float(v) for v in frame.get_column("high").to_list()]
    low = [float(v) for v in frame.get_column("low").to_list()]
    close = [float(v) for v in frame.get_column("close").to_list()]

    if not open_time:
        return _empty_result(symbol=symbol, interval=interval, config=config)

    if "volume" in frame.columns:
        volume = [max(float(v), 0.0) for v in frame.get_column("volume").to_list()]
    else:
        volume = [1.0 for _ in open_time]

    if "quote_volume" in frame.columns:
        quote_volume = [float(v) for v in frame.get_column("quote_volume").to_list()]
    elif "quote_asset_volume" in frame.columns:
        quote_volume = [float(v) for v in frame.get_column("quote_asset_volume").to_list()]
    else:
        quote_volume = [float("nan") for _ in open_time]

    vwap: list[float] = []
    for c, v, q in zip(close, volume, quote_volume):
        if v > 0 and q == q:
            vwap.append(q / v)
        else:
            vwap.append(c)

    turnover = _turnover_from_volume(
        volume=volume,
        window=config.rp_turnover_window,
        base_turnover=config.rp_base_turnover,
        max_turnover_cap=config.rp_max_turnover_cap,
    )
    atr = _rolling_mean(values=_true_range(high=high, low=low, close=close), window=config.atr_lookback)
    warmup = max(
        1,
        config.rp_entry_confirm_bars,
        config.atr_lookback if (config.use_rp_chop_filter or config.use_rp_signal_quality_sizing) else 1,
        config.rp_slope_bars if config.use_rp_chop_filter else 1,
    )

    strategy = RPDailyBreakoutStrategy(
        RPDailyBreakoutConfig(
            rp_window=config.rp_window,
            entry_confirmations=config.rp_entry_confirm_bars,
            exit_confirmations=config.rp_exit_confirm_bars,
            quantity=config.rp_quantity,
        )
    )
    prepared = strategy.prepare_features(frame)
    reference_price = [float(v) for v in prepared.get_column("rp").to_list()]
    strategy.on_start(StrategyContext(symbol=symbol, bars=prepared))

    fee_rate = config.fee_bps / 10000.0
    slippage_rate = config.slippage_bps / 10000.0

    cash = config.initial_capital
    position_qty = 0.0
    position_avg_price = 0.0
    position_entry_fee = 0.0
    entry_bar_index: int | None = None
    last_exit_bar = -10**9

    realized_pnl = 0.0
    trades: list[dict[str, object]] = []
    equity_curve: list[dict[str, object]] = []
    closed_trade_pnls: list[float] = []

    max_equity = config.initial_capital
    max_drawdown = 0.0

    equity_curve.append(
        {
            "open_time": open_time[0],
            "close": close[0],
            "equity": cash,
            "position_qty": position_qty,
            "stop_price": None,
            "realized_pnl": realized_pnl,
        }
    )

    for fill_index in range(1, len(open_time)):
        signal_index = fill_index - 1
        prev_equity = cash + position_qty * close[signal_index]
        rp_slope_ratio = 0.0
        atr_ratio = 0.0
        regime_slope = 0.0
        atr_prev = atr[signal_index]
        if signal_index >= config.rp_slope_bars and reference_price[signal_index - config.rp_slope_bars] != 0:
            rp_slope_ratio = (
                reference_price[signal_index] - reference_price[signal_index - config.rp_slope_bars]
            ) / abs(reference_price[signal_index - config.rp_slope_bars])
        if atr_prev is not None and close[signal_index] > 0:
            atr_ratio = float(atr_prev) / close[signal_index]

        if position_qty == 0.0:
            can_check_entry = (
                signal_index >= warmup
                and signal_index >= config.rp_entry_confirm_bars - 1
                and (fill_index - last_exit_bar) > config.cooldown_bars
            )
            enter_long = False
            entry_size = 1.0
            if can_check_entry:
                context = StrategyContext(symbol=symbol, bars=prepared.head(signal_index + 1))
                decisions = strategy.on_bar(context)
                enter_decision = next((d for d in decisions if d.decision_type == StrategyDecisionType.ENTER_LONG), None)
                if enter_decision is not None:
                    enter_long = True
                    entry_size = float(enter_decision.size)

            if enter_long:
                fill = _fill_price(side="BUY", base_price=open_price[fill_index], slippage_rate=slippage_rate)
                qty = prev_equity * config.max_leverage * entry_size / (fill * (1.0 + fee_rate)) if fill > 0 else 0.0
                if qty > 0:
                    fee = qty * fill * fee_rate
                    cash -= qty * fill + fee
                    position_qty = qty
                    position_avg_price = fill
                    position_entry_fee = fee
                    entry_bar_index = fill_index

                    trades.append(
                        {
                            "open_time": open_time[fill_index],
                            "side": "BUY",
                            "action": "ENTER_LONG_RP2",
                            "price": fill,
                            "qty": qty,
                            "fee": fee,
                            "rp_value": reference_price[signal_index],
                            "turnover_prev": turnover[signal_index],
                            "regime_slope": regime_slope,
                            "rp_slope_ratio": rp_slope_ratio,
                            "atr_ratio": atr_ratio,
                            "signal_quality_scale": entry_size,
                            "vol_target_allocation": config.max_leverage * entry_size,
                            "equity_after": cash + position_qty * close[fill_index],
                        }
                    )
                else:
                    strategy.set_long_open(symbol=symbol, is_open=False)

        elif position_qty > 0.0:
            hold_bars = None if entry_bar_index is None else fill_index - entry_bar_index
            hold_ok = entry_bar_index is None or hold_bars >= max(config.rp_min_hold_bars, 0)
            time_stop = (
                config.max_hold_bars is not None
                and entry_bar_index is not None
                and hold_bars >= config.max_hold_bars
            )
            exit_long = False
            if not time_stop and hold_ok:
                context = StrategyContext(symbol=symbol, bars=prepared.head(signal_index + 1))
                decisions = strategy.on_bar(context)
                exit_long = any(d.decision_type == StrategyDecisionType.EXIT_LONG for d in decisions)

            if time_stop or (exit_long and hold_ok):
                fill = _fill_price(side="SELL", base_price=open_price[fill_index], slippage_rate=slippage_rate)
                qty = abs(position_qty)
                fee = qty * fill * fee_rate
                cash += qty * fill - fee

                trade_pnl = (fill - position_avg_price) * qty - position_entry_fee - fee
                realized_pnl += trade_pnl
                closed_trade_pnls.append(trade_pnl)

                trades.append(
                    {
                        "open_time": open_time[fill_index],
                        "side": "SELL",
                        "action": "EXIT_LONG_RP2",
                        "price": fill,
                        "qty": qty,
                        "fee": fee,
                        "trade_pnl": trade_pnl,
                        "rp_value": reference_price[signal_index],
                        "turnover_prev": turnover[signal_index],
                        "hold_bars": hold_bars,
                        "exit_reason": "TIME_STOP" if time_stop else "RP_CROSS",
                        "equity_after": cash,
                    }
                )

                position_qty = 0.0
                position_avg_price = 0.0
                position_entry_fee = 0.0
                entry_bar_index = None
                last_exit_bar = fill_index
                strategy.set_long_open(symbol=symbol, is_open=False)

        mark_price = close[fill_index]
        equity = cash + position_qty * mark_price
        max_equity = max(max_equity, equity)
        if max_equity > 0:
            max_drawdown = max(max_drawdown, (max_equity - equity) / max_equity)

        equity_curve.append(
            {
                "open_time": open_time[fill_index],
                "close": mark_price,
                "equity": equity,
                "position_qty": position_qty,
                "stop_price": None,
                "realized_pnl": realized_pnl,
            }
        )

    strategy.on_finish(StrategyContext(symbol=symbol, bars=prepared))

    final_equity = cash + position_qty * close[-1]
    net_pnl = final_equity - config.initial_capital
    wins = sum(1 for pnl in closed_trade_pnls if pnl > 0)
    closed_trades = len(closed_trade_pnls)
    gross_profit = sum(pnl for pnl in closed_trade_pnls if pnl > 0)
    gross_loss = abs(sum(pnl for pnl in closed_trade_pnls if pnl < 0))
    profit_factor = None if gross_loss == 0 else gross_profit / gross_loss
    win_rate = 0.0 if closed_trades == 0 else wins / closed_trades

    return {
        "symbol": symbol,
        "interval": interval,
        "start_open_time": open_time[0],
        "end_open_time": open_time[-1],
        "config": _config_dict(config),
        "summary": {
            "initial_capital": config.initial_capital,
            "final_equity": final_equity,
            "net_pnl": net_pnl,
            "return_pct": 0.0 if config.initial_capital == 0 else net_pnl / config.initial_capital,
            "max_drawdown": max_drawdown,
            "total_trades": len(trades),
            "closed_trades": closed_trades,
            "win_rate": win_rate,
            "gross_profit": gross_profit,
            "gross_loss": gross_loss,
            "profit_factor": profit_factor,
            "realized_pnl": realized_pnl,
            "breakout_entry_alpha_pnl": 0.0,
            "pullback_entry_alpha_pnl": 0.0,
            "exit_alpha_pnl": 0.0,
            "final_position_qty": position_qty,
            "open_position_side": "LONG" if position_qty > 0 else None,
        },
        "trades": trades,
        "equity_curve": equity_curve,
    }


def write_backtest_output(output: dict[str, object], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(output, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _empty_result(*, symbol: str, interval: str, config: TurtleConfig) -> dict[str, object]:
    return {
        "symbol": symbol,
        "interval": interval,
        "start_open_time": None,
        "end_open_time": None,
        "config": _config_dict(config),
        "summary": {
            "initial_capital": config.initial_capital,
            "final_equity": config.initial_capital,
            "net_pnl": 0.0,
            "return_pct": 0.0,
            "max_drawdown": 0.0,
            "total_trades": 0,
            "closed_trades": 0,
            "win_rate": 0.0,
            "gross_profit": 0.0,
            "gross_loss": 0.0,
            "profit_factor": None,
            "realized_pnl": 0.0,
            "breakout_entry_alpha_pnl": 0.0,
            "pullback_entry_alpha_pnl": 0.0,
            "exit_alpha_pnl": 0.0,
            "final_position_qty": 0.0,
            "open_position_side": None,
        },
        "trades": [],
        "equity_curve": [],
    }


def _build_htf_ema_for_main_bars(
    *,
    main_open_time: list[int],
    main_step_ms: int,
    bars_htf: pl.DataFrame,
    ema_window: int,
) -> list[float | None]:
    required_columns = ("open_time", "close")
    missing_columns = [c for c in required_columns if c not in bars_htf.columns]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"bars_htf missing required columns: {missing}")

    htf = bars_htf.select(*required_columns).sort("open_time")
    htf_open_time = [int(v) for v in htf.get_column("open_time").to_list()]
    htf_close = [float(v) for v in htf.get_column("close").to_list()]
    if not htf_open_time:
        return [None for _ in main_open_time]

    htf_step_ms = _infer_step_ms(htf_open_time)
    htf_ema = _ema(values=htf_close, window=ema_window)

    result: list[float | None] = []
    htf_idx = -1
    for t_open in main_open_time:
        decision_time = t_open + main_step_ms
        while htf_idx + 1 < len(htf_open_time) and htf_open_time[htf_idx + 1] + htf_step_ms <= decision_time:
            htf_idx += 1

        if htf_idx < 0:
            result.append(None)
        else:
            result.append(htf_ema[htf_idx])
    return result


def _build_htf_regime_for_main_bars(
    *,
    main_open_time: list[int],
    main_step_ms: int,
    bars_htf: pl.DataFrame,
    lookback: int,
    expansion_bars: int,
    expansion_min_growth: float,
    require_channel_expansion: bool,
) -> list[dict[str, object]]:
    required_columns = ("open_time", "high", "low", "close")
    missing_columns = [c for c in required_columns if c not in bars_htf.columns]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"bars_htf missing required columns: {missing}")

    htf = bars_htf.select(*required_columns).sort("open_time")
    htf_open_time = [int(v) for v in htf.get_column("open_time").to_list()]
    htf_high = [float(v) for v in htf.get_column("high").to_list()]
    htf_low = [float(v) for v in htf.get_column("low").to_list()]
    htf_close = [float(v) for v in htf.get_column("close").to_list()]

    if not htf_open_time:
        return [{"long": False, "short": False, "htf_idx": None, "htf_open_time": None} for _ in main_open_time]

    htf_step_ms = _infer_step_ms(htf_open_time)
    htf_widths = _rolling_channel_width(high=htf_high, low=htf_low, window=lookback)

    result: list[dict[str, object]] = []
    htf_idx = -1

    for t_open in main_open_time:
        decision_time = t_open + main_step_ms

        while htf_idx + 1 < len(htf_open_time) and htf_open_time[htf_idx + 1] + htf_step_ms <= decision_time:
            htf_idx += 1

        if htf_idx < lookback:
            result.append({"long": False, "short": False, "htf_idx": None, "htf_open_time": None})
            continue

        htf_upper = max(htf_high[htf_idx - lookback : htf_idx])
        htf_lower = min(htf_low[htf_idx - lookback : htf_idx])
        htf_mid = (htf_upper + htf_lower) / 2.0

        expansion_ok = True
        if require_channel_expansion:
            expansion_ok = _channel_expansion_ok(
                widths=htf_widths,
                idx=htf_idx,
                bars=expansion_bars,
                min_growth=expansion_min_growth,
            )

        long_ok = expansion_ok and htf_close[htf_idx] > htf_mid
        short_ok = expansion_ok and htf_close[htf_idx] < htf_mid
        result.append(
            {
                "long": long_ok,
                "short": short_ok,
                "htf_idx": htf_idx,
                "htf_open_time": htf_open_time[htf_idx],
            }
        )

    return result


def _infer_step_ms(open_time: list[int]) -> int:
    if len(open_time) < 2:
        return 1
    diffs = [open_time[i] - open_time[i - 1] for i in range(1, len(open_time)) if open_time[i] > open_time[i - 1]]
    if not diffs:
        return 1
    return min(diffs)


def _fill_price(*, side: str, base_price: float, slippage_rate: float) -> float:
    if side == "BUY":
        return base_price * (1.0 + slippage_rate)
    return base_price * (1.0 - slippage_rate)


def _position_size(
    *,
    equity: float,
    atr_value: float,
    risk_per_trade: float,
    execution_price: float,
    max_leverage: float,
    fee_rate: float,
) -> float:
    if equity <= 0 or atr_value <= 0 or execution_price <= 0:
        return 0.0

    risk_cap = equity * risk_per_trade
    qty_by_risk = risk_cap / atr_value

    notional_cap = equity * max_leverage
    qty_by_notional = notional_cap / execution_price
    qty_by_cash = notional_cap / (execution_price * (1.0 + fee_rate))

    qty = min(qty_by_risk, qty_by_notional, qty_by_cash)
    return max(qty, 0.0)


def _true_range(*, high: list[float], low: list[float], close: list[float]) -> list[float]:
    values: list[float] = []
    prev_close = close[0]
    for h, l, c in zip(high, low, close):
        tr = max(h - l, abs(h - prev_close), abs(l - prev_close))
        values.append(tr)
        prev_close = c
    return values


def _rolling_mean(*, values: list[float], window: int) -> list[float | None]:
    output: list[float | None] = []
    for idx in range(len(values)):
        if idx + 1 < window:
            output.append(None)
            continue
        segment = values[idx - window + 1 : idx + 1]
        output.append(sum(segment) / window)
    return output


def _log_returns(*, close: list[float]) -> list[float]:
    if not close:
        return []
    out = [0.0]
    for i in range(1, len(close)):
        prev = max(close[i - 1], 1e-12)
        curr = max(close[i], 1e-12)
        out.append(math.log(curr / prev))
    return out


def _rolling_std(*, values: list[float], window: int) -> list[float | None]:
    out: list[float | None] = []
    for idx in range(len(values)):
        if idx + 1 < window:
            out.append(None)
            continue
        seg = values[idx - window + 1 : idx + 1]
        mean = sum(seg) / window
        var = sum((v - mean) ** 2 for v in seg) / window
        out.append(math.sqrt(var))
    return out


def _turnover_from_volume(
    *,
    volume: list[float],
    window: int,
    base_turnover: float,
    max_turnover_cap: float,
) -> list[float]:
    log_vol = [math.log(max(v, 0.0) + 1.0) for v in volume]
    turnover: list[float] = []
    for idx in range(len(log_vol)):
        start = max(0, idx - window + 1)
        segment = log_vol[start : idx + 1]
        roll_min = min(segment)
        roll_max = max(segment)
        denominator = roll_max - roll_min
        if denominator == 0:
            denominator = 1.0
        position = (log_vol[idx] - roll_min) / denominator
        estimated = position * max_turnover_cap
        t = base_turnover + estimated * (1.0 - base_turnover)
        turnover.append(min(max(t, 0.0), 0.99))
    return turnover


def _reference_price_recursive(*, vwap: list[float], turnover: list[float]) -> list[float]:
    if not vwap:
        return []

    rp = [float(vwap[0])]
    curr = float(vwap[0])
    for idx in range(1, len(vwap)):
        curr = curr * (1.0 - turnover[idx - 1]) + float(vwap[idx - 1]) * turnover[idx - 1]
        rp.append(curr)
    return rp


def _build_htf_rp_bias_for_main_bars(
    *,
    main_open_time: list[int],
    main_step_ms: int,
    bars_htf: pl.DataFrame,
    turnover_window: int,
    base_turnover: float,
    max_turnover_cap: float,
    slope_bars: int,
) -> list[dict[str, object]]:
    required_columns = ("open_time", "close")
    missing_columns = [c for c in required_columns if c not in bars_htf.columns]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"bars_htf missing required columns: {missing}")

    htf = bars_htf.sort("open_time")
    htf_open_time = [int(v) for v in htf.get_column("open_time").to_list()]
    htf_close = [float(v) for v in htf.get_column("close").to_list()]
    if not htf_open_time:
        return [{"long": False, "htf_open_time": None} for _ in main_open_time]

    if "volume" in htf.columns:
        htf_volume = [max(float(v), 0.0) for v in htf.get_column("volume").to_list()]
    else:
        htf_volume = [1.0 for _ in htf_open_time]

    if "quote_volume" in htf.columns:
        htf_quote_volume = [float(v) for v in htf.get_column("quote_volume").to_list()]
    elif "quote_asset_volume" in htf.columns:
        htf_quote_volume = [float(v) for v in htf.get_column("quote_asset_volume").to_list()]
    else:
        htf_quote_volume = [float("nan") for _ in htf_open_time]

    htf_vwap: list[float] = []
    for c, v, q in zip(htf_close, htf_volume, htf_quote_volume):
        if v > 0 and q == q:
            htf_vwap.append(q / v)
        else:
            htf_vwap.append(c)

    htf_turnover = _turnover_from_volume(
        volume=htf_volume,
        window=turnover_window,
        base_turnover=base_turnover,
        max_turnover_cap=max_turnover_cap,
    )
    htf_rp = _reference_price_recursive(vwap=htf_vwap, turnover=htf_turnover)

    htf_step_ms = _infer_step_ms(htf_open_time)

    result: list[dict[str, object]] = []
    htf_idx = -1
    for t_open in main_open_time:
        decision_time = t_open + main_step_ms
        while htf_idx + 1 < len(htf_open_time) and htf_open_time[htf_idx + 1] + htf_step_ms <= decision_time:
            htf_idx += 1

        if htf_idx < slope_bars:
            result.append({"long": False, "htf_open_time": None})
            continue

        rp_slope = htf_rp[htf_idx] - htf_rp[htf_idx - slope_bars]
        long_ok = htf_close[htf_idx] > htf_rp[htf_idx] and rp_slope > 0
        result.append(
            {
                "long": long_ok,
                "htf_open_time": htf_open_time[htf_idx],
            }
        )

    return result


def _rolling_channel_width(*, high: list[float], low: list[float], window: int) -> list[float | None]:
    output: list[float | None] = []
    for idx in range(len(high)):
        if idx < window:
            output.append(None)
            continue
        segment_high = high[idx - window : idx]
        segment_low = low[idx - window : idx]
        output.append(max(segment_high) - min(segment_low))
    return output


def _channel_expansion_ok(*, widths: list[float | None], idx: int, bars: int, min_growth: float) -> bool:
    start = idx - bars + 1
    if start < 0:
        return False

    window = widths[start : idx + 1]
    if len(window) != bars or any(v is None for v in window):
        return False

    values = [float(v) for v in window if v is not None]
    increasing = all(values[i] > values[i - 1] for i in range(1, len(values)))
    if not increasing:
        return False

    if values[0] <= 0:
        return False
    return values[-1] / values[0] >= min_growth


def _ema(*, values: list[float], window: int | None) -> list[float | None]:
    if window is None:
        return [None for _ in values]

    alpha = 2.0 / (window + 1.0)
    output: list[float | None] = []
    ema_value = 0.0

    for idx, val in enumerate(values):
        if idx == 0:
            ema_value = val
        else:
            ema_value = alpha * val + (1.0 - alpha) * ema_value
        output.append(ema_value)
    return output


def _follow_through_ok(*, close: list[float], idx: int, level: float, bars: int, direction: str) -> bool:
    start = idx - bars + 1
    if start < 0:
        return False
    window = close[start : idx + 1]
    if len(window) != bars:
        return False
    if direction == "LONG":
        return all(v > level for v in window)
    return all(v < level for v in window)


def _signal_score_ratio(
    *,
    cfg: TurtleConfig,
    trend_bucket: str,
    ema_ok: bool,
    regime_ok: bool,
    expansion_ok: bool,
    strength_ok: bool,
    follow_ok: bool,
) -> float:
    score = 0.0
    total = 0.0

    if cfg.trend_ema_window is not None:
        total += 1.0
        if ema_ok:
            score += 1.0
    if cfg.use_htf_filter:
        total += 1.0
        if regime_ok:
            score += 1.0
    if cfg.require_channel_expansion:
        total += 1.0
        if expansion_ok:
            score += 1.0
    if cfg.min_breakout_distance_atr > 0 or cfg.min_breakout_body_atr > 0:
        total += 1.0
        if strength_ok:
            score += 1.0

    total += 1.0
    if follow_ok:
        score += 1.0

    if total <= 0:
        return 1.0
    return score / total


def _realize_from_legs(
    *,
    legs: list[dict[str, float | str]],
    close_qty: float,
    close_price: float,
    close_fee: float,
    side: str,
) -> tuple[float, dict[str, float], float]:
    remaining = close_qty
    pnl_by_source: dict[str, float] = {"breakout": 0.0, "pullback": 0.0}
    consumed_entry_fee = 0.0

    if close_qty <= 0:
        return 0.0, pnl_by_source, 0.0

    while remaining > 1e-12 and legs:
        leg = legs[0]
        leg_qty = float(leg["qty"])
        take_qty = min(remaining, leg_qty)
        if take_qty <= 0:
            legs.pop(0)
            continue

        leg_entry_price = float(leg["entry_price"])
        leg_entry_fee = float(leg["entry_fee"])
        allocated_entry_fee = leg_entry_fee * (take_qty / leg_qty) if leg_qty > 0 else 0.0
        allocated_exit_fee = close_fee * (take_qty / close_qty)

        if side == "LONG":
            leg_pnl = (close_price - leg_entry_price) * take_qty - allocated_entry_fee - allocated_exit_fee
        else:
            leg_pnl = (leg_entry_price - close_price) * take_qty - allocated_entry_fee - allocated_exit_fee

        source = str(leg["source"])
        pnl_by_source[source] = pnl_by_source.get(source, 0.0) + leg_pnl
        consumed_entry_fee += allocated_entry_fee

        new_qty = leg_qty - take_qty
        if new_qty <= 1e-12:
            legs.pop(0)
        else:
            leg["qty"] = new_qty
            leg["entry_fee"] = leg_entry_fee - allocated_entry_fee

        remaining -= take_qty

    total_pnl = sum(pnl_by_source.values())
    return total_pnl, pnl_by_source, consumed_entry_fee


def _config_dict(cfg: TurtleConfig) -> dict[str, object]:
    return {
        "entry_lookback": cfg.entry_lookback,
        "exit_lookback": cfg.exit_lookback,
        "atr_lookback": cfg.atr_lookback,
        "initial_capital": cfg.initial_capital,
        "risk_per_trade": cfg.risk_per_trade,
        "fee_bps": cfg.fee_bps,
        "slippage_bps": cfg.slippage_bps,
        "stop_atr_multiple": cfg.stop_atr_multiple,
        "max_leverage": cfg.max_leverage,
        "allow_short": cfg.allow_short,
        "trend_ema_window": cfg.trend_ema_window,
        "cooldown_bars": cfg.cooldown_bars,
        "pullback_window": cfg.pullback_window,
        "pullback_tolerance_atr": cfg.pullback_tolerance_atr,
        "require_channel_expansion": cfg.require_channel_expansion,
        "expansion_bars": cfg.expansion_bars,
        "expansion_min_growth": cfg.expansion_min_growth,
        "breakout_entry_fraction": cfg.breakout_entry_fraction,
        "use_trend_strength_layering": cfg.use_trend_strength_layering,
        "strong_trend_threshold": cfg.strong_trend_threshold,
        "weak_exit_lookback": cfg.weak_exit_lookback,
        "strong_exit_lookback": cfg.strong_exit_lookback,
        "weak_pullback_tolerance_atr": cfg.weak_pullback_tolerance_atr,
        "strong_pullback_tolerance_atr": cfg.strong_pullback_tolerance_atr,
        "weak_trend_pullback_only": cfg.weak_trend_pullback_only,
        "weak_trend_no_trade": cfg.weak_trend_no_trade,
        "add_on_only_after_profit": cfg.add_on_only_after_profit,
        "addon_min_unrealized_r": cfg.addon_min_unrealized_r,
        "min_breakout_distance_atr": cfg.min_breakout_distance_atr,
        "min_breakout_body_atr": cfg.min_breakout_body_atr,
        "enable_partial_take_profit": cfg.enable_partial_take_profit,
        "take_profit_r_multiple": cfg.take_profit_r_multiple,
        "take_profit_fraction": cfg.take_profit_fraction,
        "use_signal_score_filter": cfg.use_signal_score_filter,
        "min_signal_score_ratio": cfg.min_signal_score_ratio,
        "min_position_scale": cfg.min_position_scale,
        "follow_through_bars": cfg.follow_through_bars,
        "follow_through_max_wait_bars": cfg.follow_through_max_wait_bars,
        "max_hold_bars": cfg.max_hold_bars,
        "use_htf_filter": cfg.use_htf_filter,
        "htf_entry_lookback": cfg.htf_entry_lookback,
        "htf_expansion_bars": cfg.htf_expansion_bars,
        "htf_expansion_min_growth": cfg.htf_expansion_min_growth,
        "htf_require_channel_expansion": cfg.htf_require_channel_expansion,
        "rp_turnover_window": cfg.rp_turnover_window,
        "rp_base_turnover": cfg.rp_base_turnover,
        "rp_max_turnover_cap": cfg.rp_max_turnover_cap,
        "rp_window": cfg.rp_window,
        "rp_quantity": float(cfg.rp_quantity),
        "rp_entry_confirm_bars": cfg.rp_entry_confirm_bars,
        "rp_exit_confirm_bars": cfg.rp_exit_confirm_bars,
        "rp_entry_band_atr": cfg.rp_entry_band_atr,
        "rp_exit_band_atr": cfg.rp_exit_band_atr,
        "rp_min_hold_bars": cfg.rp_min_hold_bars,
        "rp_htf_slope_bars": cfg.rp_htf_slope_bars,
        "use_rp_chop_filter": cfg.use_rp_chop_filter,
        "rp_slope_bars": cfg.rp_slope_bars,
        "rp_min_slope_ratio": cfg.rp_min_slope_ratio,
        "rp_min_atr_ratio": cfg.rp_min_atr_ratio,
        "use_rp_signal_quality_sizing": cfg.use_rp_signal_quality_sizing,
        "rp_quality_target_atr": cfg.rp_quality_target_atr,
        "rp_quality_min_scale": cfg.rp_quality_min_scale,
        "use_regime_filter": cfg.use_regime_filter,
        "regime_ema_window": cfg.regime_ema_window,
        "regime_slope_bars": cfg.regime_slope_bars,
        "regime_min_slope": cfg.regime_min_slope,
        "use_vol_target_sizing": cfg.use_vol_target_sizing,
        "target_annual_vol": cfg.target_annual_vol,
        "vol_target_window": cfg.vol_target_window,
        "min_position_allocation": cfg.min_position_allocation,
    }


def _effective_trend_params(
    *,
    cfg: TurtleConfig,
    width: float | None,
    atr_value: float,
) -> tuple[str, int, float]:
    if not cfg.use_trend_strength_layering:
        return "base", cfg.exit_lookback, cfg.pullback_tolerance_atr

    trend_strength = 0.0
    if width is not None and atr_value > 0:
        trend_strength = width / atr_value

    is_strong = trend_strength >= cfg.strong_trend_threshold
    if is_strong:
        exit_lookback = cfg.exit_lookback if cfg.strong_exit_lookback is None else cfg.strong_exit_lookback
        pullback_tol = (
            cfg.pullback_tolerance_atr
            if cfg.strong_pullback_tolerance_atr is None
            else cfg.strong_pullback_tolerance_atr
        )
        return "strong", exit_lookback, pullback_tol

    exit_lookback = cfg.exit_lookback if cfg.weak_exit_lookback is None else cfg.weak_exit_lookback
    pullback_tol = cfg.pullback_tolerance_atr if cfg.weak_pullback_tolerance_atr is None else cfg.weak_pullback_tolerance_atr
    return "weak", exit_lookback, pullback_tol
