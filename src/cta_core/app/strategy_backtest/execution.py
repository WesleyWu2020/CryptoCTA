from __future__ import annotations

import argparse
from typing import Any

from cta_core.app.turtle_backtest import TurtleConfig, run_turtle_backtest, write_backtest_output
from cta_core.config.run_config import RunConfig
from cta_core.data.market_data_store import utc_ms
from cta_core.strategy_runtime.base import StrategyContext, StrategyDecisionType
from cta_core.strategy_runtime.strategies.liquidation_vacuum_reversion import LiquidationVacuumReversionStrategy
from cta_core.strategy_runtime.strategies.liquidity_shock_reversion import LiquidityShockReversionStrategy
from cta_core.strategy_runtime.strategies.rp_daily_breakout import RPDailyBreakoutStrategy
from cta_core.strategy_runtime.strategies.rsi_threshold import RSIThresholdStrategy
from cta_core.strategy_runtime.strategies.smart_money_size_breakout import SmartMoneySizeBreakoutStrategy
from cta_core.strategy_runtime.strategies.taker_imbalance_absorption import TakerImbalanceAbsorptionStrategy

from .data_source import load_or_fetch


def _load_run_context(args: argparse.Namespace) -> tuple[RunConfig, Any, str]:
    run_cfg = RunConfig.from_args(args)
    start_ms = utc_ms(run_cfg.start)
    end_ms = utc_ms(run_cfg.end)
    bars, data_source = load_or_fetch(
        db_path=run_cfg.db_path,
        use_binance=run_cfg.use_binance,
        symbol=run_cfg.symbol,
        interval=run_cfg.interval,
        start_ms=start_ms,
        end_ms=end_ms,
    )
    return run_cfg, bars, data_source


def _print_and_write_result(result: dict[str, object], output_path: Any) -> int:
    path = write_backtest_output(result, output_path)
    print(path)
    print(result["summary"])
    return 0


def _true_range_series(*, high: list[float], low: list[float], close: list[float]) -> list[float]:
    out: list[float] = []
    for index, (high_value, low_value) in enumerate(zip(high, low)):
        if index == 0:
            out.append(high_value - low_value)
            continue
        prev_close = close[index - 1]
        out.append(max(high_value - low_value, abs(high_value - prev_close), abs(low_value - prev_close)))
    return out


def _rolling_mean(values: list[float], window: int) -> list[float | None]:
    out: list[float | None] = []
    running = 0.0
    for index, value in enumerate(values):
        running += value
        if index >= window:
            running -= values[index - window]
        if index + 1 < window:
            out.append(None)
        else:
            out.append(running / window)
    return out


def _column_as_float_list(frame: Any, column: str, default: float = 0.0) -> list[float]:
    values: list[float] = []
    for value in frame.get_column(column).to_list():
        if value is None:
            values.append(default)
        else:
            values.append(float(value))
    return values


def execute_rp_daily_breakout(args: argparse.Namespace) -> int:
    run_cfg, bars, data_source = _load_run_context(args)
    strategy_config = RPDailyBreakoutStrategy.config_from_args(args)
    turtle_config = TurtleConfig.from_flat_kwargs(
        initial_capital=run_cfg.initial_capital,
        fee_bps=run_cfg.fee_bps,
        slippage_bps=run_cfg.slippage_bps,
        max_leverage=run_cfg.max_leverage,
        rp_window=strategy_config.rp_window,
        rp_quantity=strategy_config.quantity,
        cooldown_bars=getattr(args, "cooldown_bars", 4),
        rp_entry_confirm_bars=strategy_config.entry_confirmations,
        rp_exit_confirm_bars=strategy_config.exit_confirmations,
        allow_short=getattr(args, "allow_short", False),
        max_hold_bars=getattr(args, "max_hold_bars", 40),
        use_rp_chop_filter=getattr(args, "use_rp_chop_filter", False),
        use_rp_signal_quality_sizing=getattr(args, "use_rp_signal_quality_sizing", False),
        use_vol_target_sizing=getattr(args, "use_vol_target_sizing", False),
        use_htf_filter=False,
        use_regime_filter=False,
    )
    result = run_turtle_backtest(
        bars=bars,
        bars_htf=None,
        symbol=run_cfg.symbol,
        interval=run_cfg.interval,
        config=turtle_config,
    )

    result["data_source"] = data_source
    result["data_source_htf"] = None

    return _print_and_write_result(result, run_cfg.output)


def execute_rsi_threshold(args: argparse.Namespace) -> int:
    run_cfg, bars, data_source = _load_run_context(args)
    strategy = RSIThresholdStrategy(RSIThresholdStrategy.config_from_args(args))
    atr_window = int(args.atr_window)
    atr_stop_multiplier = float(args.atr_stop_multiplier)
    atr_trailing_multiplier = float(args.atr_trailing_multiplier)
    max_hold_bars = None if args.max_hold_bars is None else int(args.max_hold_bars)
    enable_partial_take_profit = bool(args.enable_partial_take_profit)
    take_profit_r_multiple = float(args.take_profit_r_multiple)
    take_profit_fraction = float(args.take_profit_fraction)
    cooldown_bars = int(args.cooldown_bars)
    if atr_window <= 1:
        raise ValueError("atr_window must be > 1")
    if atr_stop_multiplier <= 0:
        raise ValueError("atr_stop_multiplier must be > 0")
    if atr_trailing_multiplier <= 0:
        raise ValueError("atr_trailing_multiplier must be > 0")
    if max_hold_bars is not None and max_hold_bars < 1:
        raise ValueError("max_hold_bars must be >= 1 when provided")
    if take_profit_r_multiple <= 0:
        raise ValueError("take_profit_r_multiple must be > 0")
    if not (0 < take_profit_fraction < 1):
        raise ValueError("take_profit_fraction must be in (0, 1)")
    if cooldown_bars < 0:
        raise ValueError("cooldown_bars must be >= 0")

    required_columns = ("open_time", "open", "high", "low", "close")
    missing_columns = [column for column in required_columns if column not in bars.columns]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"bars missing required columns: {missing}")

    frame = bars.sort("open_time")
    open_time = [int(value) for value in frame.get_column("open_time").to_list()]
    open_price = [float(value) for value in frame.get_column("open").to_list()]
    high = [float(value) for value in frame.get_column("high").to_list()]
    low = [float(value) for value in frame.get_column("low").to_list()]
    close = [float(value) for value in frame.get_column("close").to_list()]
    atr = _rolling_mean(_true_range_series(high=high, low=low, close=close), atr_window)

    if not open_time:
        result: dict[str, object] = {
            "symbol": run_cfg.symbol,
            "interval": run_cfg.interval,
            "start_open_time": None,
            "end_open_time": None,
            "config": {
                "rsi_window": strategy.config.rsi_window,
                "buy_threshold": strategy.config.buy_threshold,
                "sell_threshold": strategy.config.sell_threshold,
                "trend_fast_ema_window": strategy.config.trend_fast_ema_window,
                "trend_slow_ema_window": strategy.config.trend_slow_ema_window,
                "use_trend_filter": strategy.config.use_trend_filter,
                "use_momentum_mode": strategy.config.use_momentum_mode,
                "adx_window": strategy.config.adx_window,
                "adx_threshold": strategy.config.adx_threshold,
                "use_adx_filter": strategy.config.use_adx_filter,
                "adx_filter_mode": strategy.config.adx_filter_mode,
                "atr_window": atr_window,
                "atr_stop_multiplier": atr_stop_multiplier,
                "atr_trailing_multiplier": atr_trailing_multiplier,
                "max_hold_bars": max_hold_bars,
                "enable_partial_take_profit": enable_partial_take_profit,
                "take_profit_r_multiple": take_profit_r_multiple,
                "take_profit_fraction": take_profit_fraction,
                "cooldown_bars": cooldown_bars,
                "initial_capital": run_cfg.initial_capital,
                "fee_bps": run_cfg.fee_bps,
                "slippage_bps": run_cfg.slippage_bps,
                "max_leverage": run_cfg.max_leverage,
            },
            "summary": {
                "initial_capital": run_cfg.initial_capital,
                "final_equity": run_cfg.initial_capital,
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
                "final_position_qty": 0.0,
                "open_position_side": None,
            },
            "trades": [],
            "equity_curve": [],
            "data_source": data_source,
            "data_source_htf": None,
        }
        return _print_and_write_result(result, run_cfg.output)

    prepared = strategy.prepare_features(frame)
    rsi_values = prepared.get_column("rsi").to_list()
    strategy.on_start(StrategyContext(symbol=run_cfg.symbol, bars=prepared))

    fee_rate = run_cfg.fee_bps / 10000.0
    slippage_rate = run_cfg.slippage_bps / 10000.0

    cash = run_cfg.initial_capital
    position_qty = 0.0
    position_avg_price = 0.0
    position_entry_fee = 0.0
    stop_price: float | None = None
    initial_stop_distance = 0.0
    take_profit_done = False
    entry_bar_index: int | None = None
    last_exit_bar = -10**9

    realized_pnl = 0.0
    trades: list[dict[str, object]] = []
    equity_curve: list[dict[str, object]] = []
    closed_trade_pnls: list[float] = []

    max_equity = run_cfg.initial_capital
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

    for fill_index in range(1, len(open_time)):
        signal_index = fill_index - 1
        prev_equity = cash + position_qty * close[signal_index]
        atr_prev = atr[signal_index]

        if position_qty == 0.0 and (fill_index - last_exit_bar) > cooldown_bars:
            context = StrategyContext(symbol=run_cfg.symbol, bars=prepared.head(signal_index + 1))
            decisions = strategy.on_bar(context)
            enter_long = any(decision.decision_type == StrategyDecisionType.ENTER_LONG for decision in decisions)
            if enter_long and atr_prev is not None and atr_prev > 0:
                fill = open_price[fill_index] * (1.0 + slippage_rate)
                qty = prev_equity * run_cfg.max_leverage / (fill * (1.0 + fee_rate)) if fill > 0 else 0.0
                if qty > 0:
                    fee = qty * fill * fee_rate
                    cash -= qty * fill + fee
                    position_qty = qty
                    position_avg_price = fill
                    position_entry_fee = fee
                    initial_stop_distance = float(atr_prev) * atr_stop_multiplier
                    stop_price = fill - initial_stop_distance
                    take_profit_done = False
                    entry_bar_index = fill_index
                    trades.append(
                        {
                            "open_time": open_time[fill_index],
                            "side": "BUY",
                            "action": "ENTER_LONG_RSI",
                            "price": fill,
                            "qty": qty,
                            "fee": fee,
                            "rsi": rsi_values[signal_index],
                            "atr": atr_prev,
                            "stop_price": stop_price,
                            "equity_after": cash + position_qty * close[fill_index],
                        }
                    )
                else:
                    strategy.set_long_open(symbol=run_cfg.symbol, is_open=False)
            elif enter_long:
                strategy.set_long_open(symbol=run_cfg.symbol, is_open=False)

        elif position_qty > 0.0:
            if atr_prev is not None and atr_prev > 0:
                trailing_candidate = close[signal_index] - (float(atr_prev) * atr_trailing_multiplier)
                if stop_price is None:
                    stop_price = trailing_candidate
                else:
                    stop_price = max(stop_price, trailing_candidate)

            stop_triggered = stop_price is not None and low[signal_index] <= stop_price
            if (
                enable_partial_take_profit
                and not stop_triggered
                and not take_profit_done
                and initial_stop_distance > 0
                and high[signal_index] >= (position_avg_price + take_profit_r_multiple * initial_stop_distance)
            ):
                target_price = position_avg_price + take_profit_r_multiple * initial_stop_distance
                old_qty = abs(position_qty)
                tp_qty = old_qty * take_profit_fraction
                if 0 < tp_qty < old_qty:
                    fill = max(open_price[fill_index], target_price) * (1.0 - slippage_rate)
                    fee = tp_qty * fill * fee_rate
                    entry_fee_share = position_entry_fee * (tp_qty / old_qty)
                    trade_pnl = (fill - position_avg_price) * tp_qty - entry_fee_share - fee
                    cash += tp_qty * fill - fee
                    realized_pnl += trade_pnl
                    closed_trade_pnls.append(trade_pnl)
                    position_qty = old_qty - tp_qty
                    position_entry_fee = max(0.0, position_entry_fee - entry_fee_share)
                    take_profit_done = True
                    trades.append(
                        {
                            "open_time": open_time[fill_index],
                            "side": "SELL",
                            "action": "TAKE_PROFIT_LONG_RSI",
                            "price": fill,
                            "qty": tp_qty,
                            "fee": fee,
                            "trade_pnl": trade_pnl,
                            "rsi": rsi_values[signal_index],
                            "atr": atr_prev,
                            "stop_price": stop_price,
                            "target_price": target_price,
                            "equity_after": cash + position_qty * close[fill_index],
                        }
                    )

            exit_signal = False
            hold_bars = None if entry_bar_index is None else fill_index - entry_bar_index
            time_stop = max_hold_bars is not None and hold_bars is not None and hold_bars >= max_hold_bars
            if not stop_triggered:
                context = StrategyContext(symbol=run_cfg.symbol, bars=prepared.head(signal_index + 1))
                decisions = strategy.on_bar(context)
                exit_signal = any(decision.decision_type == StrategyDecisionType.EXIT_LONG for decision in decisions)

            if stop_triggered or exit_signal or time_stop:
                base_price = open_price[fill_index]
                if stop_triggered and stop_price is not None:
                    base_price = min(base_price, stop_price)
                fill = base_price * (1.0 - slippage_rate)
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
                        "action": (
                            "EXIT_LONG_STOP"
                            if stop_triggered
                            else ("EXIT_LONG_TIME" if time_stop else "EXIT_LONG_RSI")
                        ),
                        "price": fill,
                        "qty": qty,
                        "fee": fee,
                        "trade_pnl": trade_pnl,
                        "rsi": rsi_values[signal_index],
                        "atr": atr_prev,
                        "stop_price": stop_price,
                        "hold_bars": None if entry_bar_index is None else fill_index - entry_bar_index,
                        "equity_after": cash,
                    }
                )

                position_qty = 0.0
                position_avg_price = 0.0
                position_entry_fee = 0.0
                stop_price = None
                initial_stop_distance = 0.0
                take_profit_done = False
                entry_bar_index = None
                last_exit_bar = fill_index
                strategy.set_long_open(symbol=run_cfg.symbol, is_open=False)

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
                "stop_price": stop_price,
                "realized_pnl": realized_pnl,
            }
        )

    strategy.on_finish(StrategyContext(symbol=run_cfg.symbol, bars=prepared))

    final_equity = cash + position_qty * close[-1]
    net_pnl = final_equity - run_cfg.initial_capital
    wins = sum(1 for pnl in closed_trade_pnls if pnl > 0)
    closed_trades = len(closed_trade_pnls)
    gross_profit = sum(pnl for pnl in closed_trade_pnls if pnl > 0)
    gross_loss = abs(sum(pnl for pnl in closed_trade_pnls if pnl < 0))
    profit_factor = None if gross_loss == 0 else gross_profit / gross_loss
    win_rate = 0.0 if closed_trades == 0 else wins / closed_trades

    result = {
        "symbol": run_cfg.symbol,
        "interval": run_cfg.interval,
        "start_open_time": open_time[0],
        "end_open_time": open_time[-1],
        "config": {
            "rsi_window": strategy.config.rsi_window,
            "buy_threshold": strategy.config.buy_threshold,
            "sell_threshold": strategy.config.sell_threshold,
            "trend_fast_ema_window": strategy.config.trend_fast_ema_window,
            "trend_slow_ema_window": strategy.config.trend_slow_ema_window,
            "use_trend_filter": strategy.config.use_trend_filter,
            "use_momentum_mode": strategy.config.use_momentum_mode,
            "adx_window": strategy.config.adx_window,
            "adx_threshold": strategy.config.adx_threshold,
            "use_adx_filter": strategy.config.use_adx_filter,
            "adx_filter_mode": strategy.config.adx_filter_mode,
            "atr_window": atr_window,
            "atr_stop_multiplier": atr_stop_multiplier,
            "atr_trailing_multiplier": atr_trailing_multiplier,
            "max_hold_bars": max_hold_bars,
            "enable_partial_take_profit": enable_partial_take_profit,
            "take_profit_r_multiple": take_profit_r_multiple,
            "take_profit_fraction": take_profit_fraction,
            "cooldown_bars": cooldown_bars,
            "initial_capital": run_cfg.initial_capital,
            "fee_bps": run_cfg.fee_bps,
            "slippage_bps": run_cfg.slippage_bps,
            "max_leverage": run_cfg.max_leverage,
        },
        "summary": {
            "initial_capital": run_cfg.initial_capital,
            "final_equity": final_equity,
            "net_pnl": net_pnl,
            "return_pct": 0.0 if run_cfg.initial_capital == 0 else net_pnl / run_cfg.initial_capital,
            "max_drawdown": max_drawdown,
            "total_trades": len(trades),
            "closed_trades": closed_trades,
            "win_rate": win_rate,
            "gross_profit": gross_profit,
            "gross_loss": gross_loss,
            "profit_factor": profit_factor,
            "realized_pnl": realized_pnl,
            "final_position_qty": position_qty,
            "open_position_side": "LONG" if position_qty > 0 else None,
        },
        "trades": trades,
        "equity_curve": equity_curve,
        "data_source": data_source,
        "data_source_htf": None,
    }

    return _print_and_write_result(result, run_cfg.output)


def execute_liquidity_shock_reversion(args: argparse.Namespace) -> int:
    run_cfg, bars, data_source = _load_run_context(args)
    strategy = LiquidityShockReversionStrategy(LiquidityShockReversionStrategy.config_from_args(args))

    max_hold_bars = int(strategy.config.max_hold_bars)
    stop_buffer_pct = float(strategy.config.stop_buffer_pct)
    stop_mode = str(args.lsr_stop_mode)
    atr_window = int(args.lsr_atr_window)
    atr_stop_multiplier = float(args.lsr_atr_stop_multiplier)
    enable_trailing_stop = bool(args.lsr_enable_trailing_stop)
    atr_trailing_multiplier = float(args.lsr_atr_trailing_multiplier)
    enable_partial_take_profit = bool(args.lsr_enable_partial_take_profit)
    take_profit_atr_multiple = float(args.lsr_take_profit_atr_multiple)
    take_profit_fraction = float(args.lsr_take_profit_fraction)

    if max_hold_bars < 1:
        raise ValueError("lsr_max_hold_bars must be >= 1")
    if stop_buffer_pct <= 0:
        raise ValueError("lsr_stop_buffer_pct must be > 0")
    if atr_window <= 1:
        raise ValueError("lsr_atr_window must be > 1")
    if atr_stop_multiplier <= 0:
        raise ValueError("lsr_atr_stop_multiplier must be > 0")
    if atr_trailing_multiplier <= 0:
        raise ValueError("lsr_atr_trailing_multiplier must be > 0")
    if take_profit_atr_multiple <= 0:
        raise ValueError("lsr_take_profit_atr_multiple must be > 0")
    if not (0 < take_profit_fraction < 1):
        raise ValueError("lsr_take_profit_fraction must be in (0, 1)")

    required_columns = ("open_time", "open", "high", "low", "close", "volume")
    missing_columns = [column for column in required_columns if column not in bars.columns]
    if missing_columns:
        missing = ", ".join(missing_columns)
        raise ValueError(f"bars missing required columns: {missing}")

    frame = bars.sort("open_time")
    open_time = [int(value) for value in frame.get_column("open_time").to_list()]
    open_price = [float(value) for value in frame.get_column("open").to_list()]
    high = [float(value) for value in frame.get_column("high").to_list()]
    low = [float(value) for value in frame.get_column("low").to_list()]
    close = [float(value) for value in frame.get_column("close").to_list()]
    atr = _rolling_mean(_true_range_series(high=high, low=low, close=close), atr_window)

    if not open_time:
        result: dict[str, object] = {
            "symbol": run_cfg.symbol,
            "interval": run_cfg.interval,
            "start_open_time": None,
            "end_open_time": None,
            "config": {
                "lookback_bars": strategy.config.lookback_bars,
                "zscore_threshold": strategy.config.zscore_threshold,
                "volume_quantile": strategy.config.volume_quantile,
                "long_zscore_threshold": strategy.config.long_zscore_threshold,
                "short_zscore_threshold": strategy.config.short_zscore_threshold,
                "long_volume_quantile": strategy.config.long_volume_quantile,
                "short_volume_quantile": strategy.config.short_volume_quantile,
                "long_wick_body_min_ratio": strategy.config.long_wick_body_min_ratio,
                "short_wick_body_min_ratio": strategy.config.short_wick_body_min_ratio,
                "use_dynamic_zscore_threshold": strategy.config.use_dynamic_zscore_threshold,
                "dynamic_zscore_lookback": strategy.config.dynamic_zscore_lookback,
                "dynamic_zscore_min_scale": strategy.config.dynamic_zscore_min_scale,
                "dynamic_zscore_max_scale": strategy.config.dynamic_zscore_max_scale,
                "max_hold_bars": max_hold_bars,
                "stop_buffer_pct": stop_buffer_pct,
                "stop_mode": stop_mode,
                "atr_window": atr_window,
                "atr_stop_multiplier": atr_stop_multiplier,
                "enable_trailing_stop": enable_trailing_stop,
                "atr_trailing_multiplier": atr_trailing_multiplier,
                "enable_partial_take_profit": enable_partial_take_profit,
                "take_profit_atr_multiple": take_profit_atr_multiple,
                "take_profit_fraction": take_profit_fraction,
                "initial_capital": run_cfg.initial_capital,
                "fee_bps": run_cfg.fee_bps,
                "slippage_bps": run_cfg.slippage_bps,
                "max_leverage": run_cfg.max_leverage,
            },
            "summary": {
                "initial_capital": run_cfg.initial_capital,
                "final_equity": run_cfg.initial_capital,
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
                "final_position_qty": 0.0,
                "open_position_side": None,
            },
            "trades": [],
            "equity_curve": [],
            "data_source": data_source,
            "data_source_htf": None,
        }
        return _print_and_write_result(result, run_cfg.output)

    prepared = strategy.prepare_features(frame)
    zscore_values = prepared.get_column("ret_zscore").to_list()
    strategy.on_start(StrategyContext(symbol=run_cfg.symbol, bars=prepared))

    fee_rate = run_cfg.fee_bps / 10000.0
    slippage_rate = run_cfg.slippage_bps / 10000.0

    cash = run_cfg.initial_capital
    position_qty = 0.0
    position_avg_price = 0.0
    position_entry_fee = 0.0
    position_side: str | None = None
    stop_price: float | None = None
    entry_bar_index: int | None = None
    entry_atr = 0.0
    initial_stop_distance = 0.0
    take_profit_done = False

    realized_pnl = 0.0
    trades: list[dict[str, object]] = []
    equity_curve: list[dict[str, object]] = []
    closed_trade_pnls: list[float] = []

    max_equity = run_cfg.initial_capital
    max_drawdown = 0.0

    equity_curve.append(
        {
            "open_time": open_time[0],
            "close": close[0],
            "equity": cash,
            "position_qty": position_qty,
            "position_side": position_side,
            "stop_price": stop_price,
            "realized_pnl": realized_pnl,
        }
    )

    for fill_index in range(1, len(open_time)):
        signal_index = fill_index - 1
        prev_equity = cash + position_qty * close[signal_index]
        atr_prev = atr[signal_index]

        if position_qty == 0.0:
            context = StrategyContext(symbol=run_cfg.symbol, bars=prepared.head(signal_index + 1))
            decisions = strategy.on_bar(context)
            enter_long = any(decision.decision_type == StrategyDecisionType.ENTER_LONG for decision in decisions)
            enter_short = any(decision.decision_type == StrategyDecisionType.ENTER_SHORT for decision in decisions)

            if enter_long and not enter_short:
                fill = open_price[fill_index] * (1.0 + slippage_rate)
                qty = prev_equity * run_cfg.max_leverage / (fill * (1.0 + fee_rate)) if fill > 0 else 0.0
                if qty > 0:
                    fee = qty * fill * fee_rate
                    cash -= qty * fill + fee
                    position_qty = qty
                    position_avg_price = fill
                    position_entry_fee = fee
                    position_side = "LONG"
                    if stop_mode == "atr" and atr_prev is not None and atr_prev > 0:
                        entry_atr = float(atr_prev)
                        initial_stop_distance = entry_atr * atr_stop_multiplier
                        stop_price = fill - initial_stop_distance
                    else:
                        stop_price = low[signal_index] * (1.0 - stop_buffer_pct)
                        initial_stop_distance = max(0.0, fill - stop_price)
                        entry_atr = 0.0 if atr_prev is None else float(atr_prev)
                    take_profit_done = False
                    entry_bar_index = fill_index
                    strategy.set_position_side(symbol=run_cfg.symbol, side="LONG")
                    trades.append(
                        {
                            "open_time": open_time[fill_index],
                            "side": "BUY",
                            "action": "ENTER_LONG_LSR",
                            "price": fill,
                            "qty": qty,
                            "fee": fee,
                            "ret_zscore": zscore_values[signal_index],
                            "atr": atr_prev,
                            "stop_price": stop_price,
                            "stop_mode": stop_mode,
                            "equity_after": cash + position_qty * close[fill_index],
                        }
                    )
                else:
                    strategy.set_position_side(symbol=run_cfg.symbol, side=None)

            elif enter_short and not enter_long:
                fill = open_price[fill_index] * (1.0 - slippage_rate)
                qty = prev_equity * run_cfg.max_leverage / (fill * (1.0 + fee_rate)) if fill > 0 else 0.0
                if qty > 0:
                    fee = qty * fill * fee_rate
                    cash += qty * fill - fee
                    position_qty = -qty
                    position_avg_price = fill
                    position_entry_fee = fee
                    position_side = "SHORT"
                    if stop_mode == "atr" and atr_prev is not None and atr_prev > 0:
                        entry_atr = float(atr_prev)
                        initial_stop_distance = entry_atr * atr_stop_multiplier
                        stop_price = fill + initial_stop_distance
                    else:
                        stop_price = high[signal_index] * (1.0 + stop_buffer_pct)
                        initial_stop_distance = max(0.0, stop_price - fill)
                        entry_atr = 0.0 if atr_prev is None else float(atr_prev)
                    take_profit_done = False
                    entry_bar_index = fill_index
                    strategy.set_position_side(symbol=run_cfg.symbol, side="SHORT")
                    trades.append(
                        {
                            "open_time": open_time[fill_index],
                            "side": "SELL",
                            "action": "ENTER_SHORT_LSR",
                            "price": fill,
                            "qty": qty,
                            "fee": fee,
                            "ret_zscore": zscore_values[signal_index],
                            "atr": atr_prev,
                            "stop_price": stop_price,
                            "stop_mode": stop_mode,
                            "equity_after": cash + position_qty * close[fill_index],
                        }
                    )
                else:
                    strategy.set_position_side(symbol=run_cfg.symbol, side=None)

        else:
            if enable_trailing_stop and atr_prev is not None and atr_prev > 0:
                if position_side == "LONG":
                    trailing_candidate = close[signal_index] - (float(atr_prev) * atr_trailing_multiplier)
                    if stop_price is None:
                        stop_price = trailing_candidate
                    else:
                        stop_price = max(stop_price, trailing_candidate)
                else:
                    trailing_candidate = close[signal_index] + (float(atr_prev) * atr_trailing_multiplier)
                    if stop_price is None:
                        stop_price = trailing_candidate
                    else:
                        stop_price = min(stop_price, trailing_candidate)

            stop_triggered = False
            if position_side == "LONG" and stop_price is not None:
                stop_triggered = low[signal_index] <= stop_price
            elif position_side == "SHORT" and stop_price is not None:
                stop_triggered = high[signal_index] >= stop_price

            if enable_partial_take_profit and (not stop_triggered) and (not take_profit_done) and entry_atr > 0:
                target_distance = take_profit_atr_multiple * entry_atr
                old_qty = abs(position_qty)
                tp_qty = old_qty * take_profit_fraction
                if 0 < tp_qty < old_qty:
                    if position_side == "LONG" and high[signal_index] >= (position_avg_price + target_distance):
                        target_price = position_avg_price + target_distance
                        fill = max(open_price[fill_index], target_price) * (1.0 - slippage_rate)
                        fee = tp_qty * fill * fee_rate
                        entry_fee_share = position_entry_fee * (tp_qty / old_qty)
                        trade_pnl = (fill - position_avg_price) * tp_qty - entry_fee_share - fee
                        cash += tp_qty * fill - fee
                        realized_pnl += trade_pnl
                        closed_trade_pnls.append(trade_pnl)
                        position_qty = old_qty - tp_qty
                        position_entry_fee = max(0.0, position_entry_fee - entry_fee_share)
                        take_profit_done = True
                        trades.append(
                            {
                                "open_time": open_time[fill_index],
                                "side": "SELL",
                                "action": "TAKE_PROFIT_LONG_LSR",
                                "price": fill,
                                "qty": tp_qty,
                                "fee": fee,
                                "trade_pnl": trade_pnl,
                                "ret_zscore": zscore_values[signal_index],
                                "atr": atr_prev,
                                "target_price": target_price,
                                "stop_price": stop_price,
                                "equity_after": cash + position_qty * close[fill_index],
                            }
                        )
                    elif position_side == "SHORT" and low[signal_index] <= (position_avg_price - target_distance):
                        target_price = position_avg_price - target_distance
                        fill = min(open_price[fill_index], target_price) * (1.0 + slippage_rate)
                        fee = tp_qty * fill * fee_rate
                        entry_fee_share = position_entry_fee * (tp_qty / old_qty)
                        trade_pnl = (position_avg_price - fill) * tp_qty - entry_fee_share - fee
                        cash -= tp_qty * fill + fee
                        realized_pnl += trade_pnl
                        closed_trade_pnls.append(trade_pnl)
                        position_qty = -(old_qty - tp_qty)
                        position_entry_fee = max(0.0, position_entry_fee - entry_fee_share)
                        take_profit_done = True
                        trades.append(
                            {
                                "open_time": open_time[fill_index],
                                "side": "BUY",
                                "action": "TAKE_PROFIT_SHORT_LSR",
                                "price": fill,
                                "qty": tp_qty,
                                "fee": fee,
                                "trade_pnl": trade_pnl,
                                "ret_zscore": zscore_values[signal_index],
                                "atr": atr_prev,
                                "target_price": target_price,
                                "stop_price": stop_price,
                                "equity_after": cash + position_qty * close[fill_index],
                            }
                        )

            hold_bars = None if entry_bar_index is None else fill_index - entry_bar_index
            time_stop = hold_bars is not None and hold_bars >= max_hold_bars

            if stop_triggered or time_stop:
                qty = abs(position_qty)
                if position_side == "LONG":
                    base_price = open_price[fill_index]
                    if stop_triggered and stop_price is not None:
                        base_price = min(base_price, stop_price)
                    fill = base_price * (1.0 - slippage_rate)
                    fee = qty * fill * fee_rate
                    cash += qty * fill - fee
                    trade_pnl = (fill - position_avg_price) * qty - position_entry_fee - fee
                    action = "EXIT_LONG_STOP" if stop_triggered else "EXIT_LONG_TIME"
                    side = "SELL"
                else:
                    base_price = open_price[fill_index]
                    if stop_triggered and stop_price is not None:
                        base_price = max(base_price, stop_price)
                    fill = base_price * (1.0 + slippage_rate)
                    fee = qty * fill * fee_rate
                    cash -= qty * fill + fee
                    trade_pnl = (position_avg_price - fill) * qty - position_entry_fee - fee
                    action = "EXIT_SHORT_STOP" if stop_triggered else "EXIT_SHORT_TIME"
                    side = "BUY"

                realized_pnl += trade_pnl
                closed_trade_pnls.append(trade_pnl)
                trades.append(
                    {
                        "open_time": open_time[fill_index],
                        "side": side,
                        "action": action,
                        "price": fill,
                        "qty": qty,
                        "fee": fee,
                        "trade_pnl": trade_pnl,
                        "ret_zscore": zscore_values[signal_index],
                        "atr": atr_prev,
                        "stop_price": stop_price,
                        "hold_bars": hold_bars,
                        "equity_after": cash,
                    }
                )

                position_qty = 0.0
                position_avg_price = 0.0
                position_entry_fee = 0.0
                position_side = None
                stop_price = None
                entry_bar_index = None
                entry_atr = 0.0
                initial_stop_distance = 0.0
                take_profit_done = False
                strategy.set_position_side(symbol=run_cfg.symbol, side=None)

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
                "position_side": position_side,
                "stop_price": stop_price,
                "realized_pnl": realized_pnl,
            }
        )

    strategy.on_finish(StrategyContext(symbol=run_cfg.symbol, bars=prepared))

    final_equity = cash + position_qty * close[-1]
    net_pnl = final_equity - run_cfg.initial_capital
    wins = sum(1 for pnl in closed_trade_pnls if pnl > 0)
    closed_trades = len(closed_trade_pnls)
    gross_profit = sum(pnl for pnl in closed_trade_pnls if pnl > 0)
    gross_loss = abs(sum(pnl for pnl in closed_trade_pnls if pnl < 0))
    profit_factor = None if gross_loss == 0 else gross_profit / gross_loss
    win_rate = 0.0 if closed_trades == 0 else wins / closed_trades

    result = {
        "symbol": run_cfg.symbol,
        "interval": run_cfg.interval,
        "start_open_time": open_time[0],
        "end_open_time": open_time[-1],
        "config": {
            "lookback_bars": strategy.config.lookback_bars,
            "zscore_threshold": strategy.config.zscore_threshold,
            "volume_quantile": strategy.config.volume_quantile,
            "long_zscore_threshold": strategy.config.long_zscore_threshold,
            "short_zscore_threshold": strategy.config.short_zscore_threshold,
            "long_volume_quantile": strategy.config.long_volume_quantile,
            "short_volume_quantile": strategy.config.short_volume_quantile,
            "long_wick_body_min_ratio": strategy.config.long_wick_body_min_ratio,
            "short_wick_body_min_ratio": strategy.config.short_wick_body_min_ratio,
            "use_dynamic_zscore_threshold": strategy.config.use_dynamic_zscore_threshold,
            "dynamic_zscore_lookback": strategy.config.dynamic_zscore_lookback,
            "dynamic_zscore_min_scale": strategy.config.dynamic_zscore_min_scale,
            "dynamic_zscore_max_scale": strategy.config.dynamic_zscore_max_scale,
            "max_hold_bars": max_hold_bars,
            "stop_buffer_pct": stop_buffer_pct,
            "stop_mode": stop_mode,
            "atr_window": atr_window,
            "atr_stop_multiplier": atr_stop_multiplier,
            "enable_trailing_stop": enable_trailing_stop,
            "atr_trailing_multiplier": atr_trailing_multiplier,
            "enable_partial_take_profit": enable_partial_take_profit,
            "take_profit_atr_multiple": take_profit_atr_multiple,
            "take_profit_fraction": take_profit_fraction,
            "initial_capital": run_cfg.initial_capital,
            "fee_bps": run_cfg.fee_bps,
            "slippage_bps": run_cfg.slippage_bps,
            "max_leverage": run_cfg.max_leverage,
        },
        "summary": {
            "initial_capital": run_cfg.initial_capital,
            "final_equity": final_equity,
            "net_pnl": net_pnl,
            "return_pct": 0.0 if run_cfg.initial_capital == 0 else net_pnl / run_cfg.initial_capital,
            "max_drawdown": max_drawdown,
            "total_trades": len(trades),
            "closed_trades": closed_trades,
            "win_rate": win_rate,
            "gross_profit": gross_profit,
            "gross_loss": gross_loss,
            "profit_factor": profit_factor,
            "realized_pnl": realized_pnl,
            "final_position_qty": position_qty,
            "open_position_side": position_side,
        },
        "trades": trades,
        "equity_curve": equity_curve,
        "data_source": data_source,
        "data_source_htf": None,
    }
    return _print_and_write_result(result, run_cfg.output)


def execute_taker_imbalance_absorption(args: argparse.Namespace) -> int:
    run_cfg, bars, data_source = _load_run_context(args)
    strategy = TakerImbalanceAbsorptionStrategy(TakerImbalanceAbsorptionStrategy.config_from_args(args))
    max_hold_bars = int(strategy.config.max_hold_bars)

    required_columns = ("open_time", "open", "high", "low", "close", "volume", "taker_buy_base_volume")
    missing_columns = [column for column in required_columns if column not in bars.columns]
    if missing_columns:
        raise ValueError(f"bars missing required columns: {', '.join(missing_columns)}")

    frame = bars.sort("open_time")
    open_time = [int(value) for value in frame.get_column("open_time").to_list()]
    open_price = [float(value) for value in frame.get_column("open").to_list()]
    close = [float(value) for value in frame.get_column("close").to_list()]
    taker_buy = _column_as_float_list(frame, "taker_buy_base_volume", default=0.0)
    volume = _column_as_float_list(frame, "volume", default=0.0)

    if not open_time:
        result: dict[str, object] = {
            "symbol": run_cfg.symbol,
            "interval": run_cfg.interval,
            "start_open_time": None,
            "end_open_time": None,
            "config": {
                "volume_ma_window": strategy.config.volume_ma_window,
                "min_taker_buy_ratio": strategy.config.min_taker_buy_ratio,
                "close_location_max": strategy.config.close_location_max,
                "max_hold_bars": max_hold_bars,
                "initial_capital": run_cfg.initial_capital,
                "fee_bps": run_cfg.fee_bps,
                "slippage_bps": run_cfg.slippage_bps,
                "max_leverage": run_cfg.max_leverage,
            },
            "summary": {
                "initial_capital": run_cfg.initial_capital,
                "final_equity": run_cfg.initial_capital,
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
                "final_position_qty": 0.0,
                "open_position_side": None,
            },
            "trades": [],
            "equity_curve": [],
            "data_source": data_source,
            "data_source_htf": None,
        }
        return _print_and_write_result(result, run_cfg.output)

    prepared = strategy.prepare_features(frame)
    strategy.on_start(StrategyContext(symbol=run_cfg.symbol, bars=prepared))

    fee_rate = run_cfg.fee_bps / 10000.0
    slippage_rate = run_cfg.slippage_bps / 10000.0
    cash = run_cfg.initial_capital
    position_qty = 0.0
    entry_price = 0.0
    entry_fee = 0.0
    entry_bar_index: int | None = None
    realized_pnl = 0.0
    trades: list[dict[str, object]] = []
    equity_curve: list[dict[str, object]] = []
    closed_trade_pnls: list[float] = []
    max_equity = run_cfg.initial_capital
    max_drawdown = 0.0

    equity_curve.append({"open_time": open_time[0], "close": close[0], "equity": cash, "position_qty": 0.0})
    for fill_index in range(1, len(open_time)):
        signal_index = fill_index - 1
        prev_equity = cash + position_qty * close[signal_index]

        if position_qty == 0.0:
            decisions = strategy.on_bar(StrategyContext(symbol=run_cfg.symbol, bars=prepared.head(signal_index + 1)))
            enter_short = any(decision.decision_type == StrategyDecisionType.ENTER_SHORT for decision in decisions)
            if enter_short:
                fill = open_price[fill_index] * (1.0 - slippage_rate)
                qty = prev_equity * run_cfg.max_leverage / (fill * (1.0 + fee_rate)) if fill > 0 else 0.0
                if qty > 0:
                    fee = qty * fill * fee_rate
                    cash += qty * fill - fee
                    position_qty = -qty
                    entry_price = fill
                    entry_fee = fee
                    entry_bar_index = fill_index
                    trades.append(
                        {
                            "open_time": open_time[fill_index],
                            "side": "SELL",
                            "action": "ENTER_SHORT_TIA",
                            "price": fill,
                            "qty": qty,
                            "fee": fee,
                            "taker_buy_ratio": 0.0 if volume[signal_index] <= 0 else taker_buy[signal_index] / volume[signal_index],
                            "equity_after": cash + position_qty * close[fill_index],
                        }
                    )
                else:
                    strategy.set_short_open(symbol=run_cfg.symbol, is_open=False)
        else:
            hold_bars = None if entry_bar_index is None else fill_index - entry_bar_index
            if hold_bars is not None and hold_bars >= max_hold_bars:
                qty = abs(position_qty)
                fill = open_price[fill_index] * (1.0 + slippage_rate)
                fee = qty * fill * fee_rate
                cash -= qty * fill + fee
                pnl = (entry_price - fill) * qty - entry_fee - fee
                realized_pnl += pnl
                closed_trade_pnls.append(pnl)
                trades.append(
                    {
                        "open_time": open_time[fill_index],
                        "side": "BUY",
                        "action": "EXIT_SHORT_TIME_TIA",
                        "price": fill,
                        "qty": qty,
                        "fee": fee,
                        "trade_pnl": pnl,
                        "hold_bars": hold_bars,
                        "equity_after": cash,
                    }
                )
                position_qty = 0.0
                entry_price = 0.0
                entry_fee = 0.0
                entry_bar_index = None
                strategy.set_short_open(symbol=run_cfg.symbol, is_open=False)

        equity = cash + position_qty * close[fill_index]
        max_equity = max(max_equity, equity)
        if max_equity > 0:
            max_drawdown = max(max_drawdown, (max_equity - equity) / max_equity)
        equity_curve.append({"open_time": open_time[fill_index], "close": close[fill_index], "equity": equity, "position_qty": position_qty})

    strategy.on_finish(StrategyContext(symbol=run_cfg.symbol, bars=prepared))
    final_equity = cash + position_qty * close[-1]
    net_pnl = final_equity - run_cfg.initial_capital
    wins = sum(1 for value in closed_trade_pnls if value > 0)
    gross_profit = sum(value for value in closed_trade_pnls if value > 0)
    gross_loss = abs(sum(value for value in closed_trade_pnls if value < 0))
    closed_trades = len(closed_trade_pnls)
    result = {
        "symbol": run_cfg.symbol,
        "interval": run_cfg.interval,
        "start_open_time": open_time[0],
        "end_open_time": open_time[-1],
        "config": {
            "volume_ma_window": strategy.config.volume_ma_window,
            "min_taker_buy_ratio": strategy.config.min_taker_buy_ratio,
            "close_location_max": strategy.config.close_location_max,
            "max_hold_bars": max_hold_bars,
            "initial_capital": run_cfg.initial_capital,
            "fee_bps": run_cfg.fee_bps,
            "slippage_bps": run_cfg.slippage_bps,
            "max_leverage": run_cfg.max_leverage,
        },
        "summary": {
            "initial_capital": run_cfg.initial_capital,
            "final_equity": final_equity,
            "net_pnl": net_pnl,
            "return_pct": 0.0 if run_cfg.initial_capital == 0 else net_pnl / run_cfg.initial_capital,
            "max_drawdown": max_drawdown,
            "total_trades": len(trades),
            "closed_trades": closed_trades,
            "win_rate": 0.0 if closed_trades == 0 else wins / closed_trades,
            "gross_profit": gross_profit,
            "gross_loss": gross_loss,
            "profit_factor": None if gross_loss == 0 else gross_profit / gross_loss,
            "realized_pnl": realized_pnl,
            "final_position_qty": position_qty,
            "open_position_side": "SHORT" if position_qty < 0 else None,
        },
        "trades": trades,
        "equity_curve": equity_curve,
        "data_source": data_source,
        "data_source_htf": None,
    }
    return _print_and_write_result(result, run_cfg.output)


def execute_liquidation_vacuum_reversion(args: argparse.Namespace) -> int:
    run_cfg, bars, data_source = _load_run_context(args)
    strategy = LiquidationVacuumReversionStrategy(LiquidationVacuumReversionStrategy.config_from_args(args))
    max_hold_bars = int(strategy.config.max_hold_bars)

    required_columns = ("open_time", "open", "high", "low", "close", "volume", "taker_buy_base_volume")
    missing_columns = [column for column in required_columns if column not in bars.columns]
    if missing_columns:
        raise ValueError(f"bars missing required columns: {', '.join(missing_columns)}")

    frame = bars.sort("open_time")
    open_time = [int(value) for value in frame.get_column("open_time").to_list()]
    open_price = [float(value) for value in frame.get_column("open").to_list()]
    close = [float(value) for value in frame.get_column("close").to_list()]

    if not open_time:
        return _print_and_write_result(
            {
                "symbol": run_cfg.symbol,
                "interval": run_cfg.interval,
                "start_open_time": None,
                "end_open_time": None,
                "config": {
                    "volume_peak_window": strategy.config.volume_peak_window,
                    "min_range_pct": strategy.config.min_range_pct,
                    "min_taker_sell_ratio": strategy.config.min_taker_sell_ratio,
                    "max_hold_bars": max_hold_bars,
                    "initial_capital": run_cfg.initial_capital,
                    "fee_bps": run_cfg.fee_bps,
                    "slippage_bps": run_cfg.slippage_bps,
                    "max_leverage": run_cfg.max_leverage,
                },
                "summary": {
                    "initial_capital": run_cfg.initial_capital,
                    "final_equity": run_cfg.initial_capital,
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
                    "final_position_qty": 0.0,
                    "open_position_side": None,
                },
                "trades": [],
                "equity_curve": [],
                "data_source": data_source,
                "data_source_htf": None,
            },
            run_cfg.output,
        )

    prepared = strategy.prepare_features(frame)
    strategy.on_start(StrategyContext(symbol=run_cfg.symbol, bars=prepared))
    fee_rate = run_cfg.fee_bps / 10000.0
    slippage_rate = run_cfg.slippage_bps / 10000.0
    cash = run_cfg.initial_capital
    position_qty = 0.0
    entry_price = 0.0
    entry_fee = 0.0
    entry_bar_index: int | None = None
    realized_pnl = 0.0
    trades: list[dict[str, object]] = []
    equity_curve: list[dict[str, object]] = []
    closed_trade_pnls: list[float] = []
    max_equity = run_cfg.initial_capital
    max_drawdown = 0.0

    equity_curve.append({"open_time": open_time[0], "close": close[0], "equity": cash, "position_qty": 0.0})
    for fill_index in range(1, len(open_time)):
        signal_index = fill_index - 1
        prev_equity = cash + position_qty * close[signal_index]

        if position_qty == 0.0:
            decisions = strategy.on_bar(StrategyContext(symbol=run_cfg.symbol, bars=prepared.head(signal_index + 1)))
            enter_long = any(decision.decision_type == StrategyDecisionType.ENTER_LONG for decision in decisions)
            if enter_long:
                fill = open_price[fill_index] * (1.0 + slippage_rate)
                qty = prev_equity * run_cfg.max_leverage / (fill * (1.0 + fee_rate)) if fill > 0 else 0.0
                if qty > 0:
                    fee = qty * fill * fee_rate
                    cash -= qty * fill + fee
                    position_qty = qty
                    entry_price = fill
                    entry_fee = fee
                    entry_bar_index = fill_index
                    trades.append(
                        {
                            "open_time": open_time[fill_index],
                            "side": "BUY",
                            "action": "ENTER_LONG_LVR",
                            "price": fill,
                            "qty": qty,
                            "fee": fee,
                            "equity_after": cash + position_qty * close[fill_index],
                        }
                    )
                else:
                    strategy.set_long_open(symbol=run_cfg.symbol, is_open=False)
        else:
            hold_bars = None if entry_bar_index is None else fill_index - entry_bar_index
            fail_confirm = hold_bars == 1 and close[signal_index] <= open_price[signal_index]
            time_stop = hold_bars is not None and hold_bars >= max_hold_bars
            if fail_confirm or time_stop:
                qty = abs(position_qty)
                fill = open_price[fill_index] * (1.0 - slippage_rate)
                fee = qty * fill * fee_rate
                cash += qty * fill - fee
                pnl = (fill - entry_price) * qty - entry_fee - fee
                realized_pnl += pnl
                closed_trade_pnls.append(pnl)
                trades.append(
                    {
                        "open_time": open_time[fill_index],
                        "side": "SELL",
                        "action": "EXIT_LONG_FAIL_CONFIRM_LVR" if fail_confirm else "EXIT_LONG_TIME_LVR",
                        "price": fill,
                        "qty": qty,
                        "fee": fee,
                        "trade_pnl": pnl,
                        "hold_bars": hold_bars,
                        "equity_after": cash,
                    }
                )
                position_qty = 0.0
                entry_price = 0.0
                entry_fee = 0.0
                entry_bar_index = None
                strategy.set_long_open(symbol=run_cfg.symbol, is_open=False)

        equity = cash + position_qty * close[fill_index]
        max_equity = max(max_equity, equity)
        if max_equity > 0:
            max_drawdown = max(max_drawdown, (max_equity - equity) / max_equity)
        equity_curve.append({"open_time": open_time[fill_index], "close": close[fill_index], "equity": equity, "position_qty": position_qty})

    strategy.on_finish(StrategyContext(symbol=run_cfg.symbol, bars=prepared))
    final_equity = cash + position_qty * close[-1]
    net_pnl = final_equity - run_cfg.initial_capital
    wins = sum(1 for value in closed_trade_pnls if value > 0)
    gross_profit = sum(value for value in closed_trade_pnls if value > 0)
    gross_loss = abs(sum(value for value in closed_trade_pnls if value < 0))
    closed_trades = len(closed_trade_pnls)
    return _print_and_write_result(
        {
            "symbol": run_cfg.symbol,
            "interval": run_cfg.interval,
            "start_open_time": open_time[0],
            "end_open_time": open_time[-1],
            "config": {
                "volume_peak_window": strategy.config.volume_peak_window,
                "min_range_pct": strategy.config.min_range_pct,
                "min_taker_sell_ratio": strategy.config.min_taker_sell_ratio,
                "max_hold_bars": max_hold_bars,
                "initial_capital": run_cfg.initial_capital,
                "fee_bps": run_cfg.fee_bps,
                "slippage_bps": run_cfg.slippage_bps,
                "max_leverage": run_cfg.max_leverage,
            },
            "summary": {
                "initial_capital": run_cfg.initial_capital,
                "final_equity": final_equity,
                "net_pnl": net_pnl,
                "return_pct": 0.0 if run_cfg.initial_capital == 0 else net_pnl / run_cfg.initial_capital,
                "max_drawdown": max_drawdown,
                "total_trades": len(trades),
                "closed_trades": closed_trades,
                "win_rate": 0.0 if closed_trades == 0 else wins / closed_trades,
                "gross_profit": gross_profit,
                "gross_loss": gross_loss,
                "profit_factor": None if gross_loss == 0 else gross_profit / gross_loss,
                "realized_pnl": realized_pnl,
                "final_position_qty": position_qty,
                "open_position_side": "LONG" if position_qty > 0 else None,
            },
            "trades": trades,
            "equity_curve": equity_curve,
            "data_source": data_source,
            "data_source_htf": None,
        },
        run_cfg.output,
    )


def execute_smart_money_size_breakout(args: argparse.Namespace) -> int:
    run_cfg, bars, data_source = _load_run_context(args)
    strategy = SmartMoneySizeBreakoutStrategy(SmartMoneySizeBreakoutStrategy.config_from_args(args))
    max_hold_bars = int(strategy.config.max_hold_bars)
    reversal_max_hold_bars = int(strategy.config.reversal_max_hold_bars)
    reversal_stop_buffer_pct = float(strategy.config.reversal_stop_buffer_pct)

    required_columns = ("open_time", "open", "high", "low", "close", "volume", "trades_count", "taker_buy_base_volume")
    missing_columns = [column for column in required_columns if column not in bars.columns]
    if missing_columns:
        raise ValueError(f"bars missing required columns: {', '.join(missing_columns)}")

    frame = bars.sort("open_time")
    open_time = [int(value) for value in frame.get_column("open_time").to_list()]
    open_price = [float(value) for value in frame.get_column("open").to_list()]
    high = [float(value) for value in frame.get_column("high").to_list()]
    close = [float(value) for value in frame.get_column("close").to_list()]
    taker_buy = _column_as_float_list(frame, "taker_buy_base_volume", default=0.0)
    volume = _column_as_float_list(frame, "volume", default=0.0)

    if not open_time:
        return _print_and_write_result(
            {
                "symbol": run_cfg.symbol,
                "interval": run_cfg.interval,
                "start_open_time": None,
                "end_open_time": None,
                "config": {
                    "avg_trade_size_window": strategy.config.avg_trade_size_window,
                    "size_zscore_threshold": strategy.config.size_zscore_threshold,
                    "min_taker_buy_ratio": strategy.config.min_taker_buy_ratio,
                    "entry_confirm_buy_ratio_threshold": strategy.config.entry_confirm_buy_ratio_threshold,
                    "close_to_high_threshold": strategy.config.close_to_high_threshold,
                    "exit_buy_ratio_threshold": strategy.config.exit_buy_ratio_threshold,
                    "max_hold_bars": max_hold_bars,
                    "enable_failed_breakout_reversal": strategy.config.enable_failed_breakout_reversal,
                    "reversal_trigger_buy_ratio_threshold": strategy.config.reversal_trigger_buy_ratio_threshold,
                    "reversal_close_location_max": strategy.config.reversal_close_location_max,
                    "reversal_exit_buy_ratio_threshold": strategy.config.reversal_exit_buy_ratio_threshold,
                    "reversal_max_hold_bars": reversal_max_hold_bars,
                    "reversal_stop_buffer_pct": reversal_stop_buffer_pct,
                    "initial_capital": run_cfg.initial_capital,
                    "fee_bps": run_cfg.fee_bps,
                    "slippage_bps": run_cfg.slippage_bps,
                    "max_leverage": run_cfg.max_leverage,
                },
                "summary": {
                    "initial_capital": run_cfg.initial_capital,
                    "final_equity": run_cfg.initial_capital,
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
                    "final_position_qty": 0.0,
                    "open_position_side": None,
                },
                "trades": [],
                "equity_curve": [],
                "data_source": data_source,
                "data_source_htf": None,
            },
            run_cfg.output,
        )

    prepared = strategy.prepare_features(frame)
    strategy.on_start(StrategyContext(symbol=run_cfg.symbol, bars=prepared))
    fee_rate = run_cfg.fee_bps / 10000.0
    slippage_rate = run_cfg.slippage_bps / 10000.0
    cash = run_cfg.initial_capital
    position_qty = 0.0
    position_side: str | None = None
    entry_price = 0.0
    entry_fee = 0.0
    entry_bar_index: int | None = None
    stop_price: float | None = None
    realized_pnl = 0.0
    trades: list[dict[str, object]] = []
    equity_curve: list[dict[str, object]] = []
    closed_trade_pnls: list[float] = []
    max_equity = run_cfg.initial_capital
    max_drawdown = 0.0

    equity_curve.append(
        {
            "open_time": open_time[0],
            "close": close[0],
            "equity": cash,
            "position_qty": 0.0,
            "position_side": None,
            "stop_price": None,
        }
    )
    for fill_index in range(1, len(open_time)):
        signal_index = fill_index - 1
        prev_equity = cash + position_qty * close[signal_index]
        decisions = strategy.on_bar(StrategyContext(symbol=run_cfg.symbol, bars=prepared.head(signal_index + 1)))

        if position_qty == 0.0:
            enter_long = any(decision.decision_type == StrategyDecisionType.ENTER_LONG for decision in decisions)
            enter_short = any(decision.decision_type == StrategyDecisionType.ENTER_SHORT for decision in decisions)
            if enter_long and not enter_short:
                fill = open_price[fill_index] * (1.0 + slippage_rate)
                qty = prev_equity * run_cfg.max_leverage / (fill * (1.0 + fee_rate)) if fill > 0 else 0.0
                if qty > 0:
                    fee = qty * fill * fee_rate
                    cash -= qty * fill + fee
                    position_qty = qty
                    position_side = "LONG"
                    entry_price = fill
                    entry_fee = fee
                    entry_bar_index = fill_index
                    stop_price = None
                    trades.append(
                        {
                            "open_time": open_time[fill_index],
                            "side": "BUY",
                            "action": "ENTER_LONG_SMB",
                            "price": fill,
                            "qty": qty,
                            "fee": fee,
                            "taker_buy_ratio": 0.0 if volume[signal_index] <= 0 else taker_buy[signal_index] / volume[signal_index],
                            "equity_after": cash + position_qty * close[fill_index],
                        }
                    )
                else:
                    strategy.set_long_open(symbol=run_cfg.symbol, is_open=False)
            elif enter_short and not enter_long:
                fill = open_price[fill_index] * (1.0 - slippage_rate)
                qty = prev_equity * run_cfg.max_leverage / (fill * (1.0 + fee_rate)) if fill > 0 else 0.0
                if qty > 0:
                    fee = qty * fill * fee_rate
                    cash += qty * fill - fee
                    position_qty = -qty
                    position_side = "SHORT"
                    entry_price = fill
                    entry_fee = fee
                    entry_bar_index = fill_index
                    stop_price = high[signal_index] * (1.0 + reversal_stop_buffer_pct)
                    trades.append(
                        {
                            "open_time": open_time[fill_index],
                            "side": "SELL",
                            "action": "ENTER_SHORT_REV_SMB",
                            "price": fill,
                            "qty": qty,
                            "fee": fee,
                            "taker_buy_ratio": 0.0 if volume[signal_index] <= 0 else taker_buy[signal_index] / volume[signal_index],
                            "stop_price": stop_price,
                            "equity_after": cash + position_qty * close[fill_index],
                        }
                    )
                else:
                    strategy.set_short_open(symbol=run_cfg.symbol, is_open=False)
        else:
            hold_bars = None if entry_bar_index is None else fill_index - entry_bar_index
            if position_side == "LONG":
                exit_signal = any(decision.decision_type == StrategyDecisionType.EXIT_LONG for decision in decisions)
                time_stop = hold_bars is not None and hold_bars >= max_hold_bars
                if exit_signal or time_stop:
                    qty = abs(position_qty)
                    fill = open_price[fill_index] * (1.0 - slippage_rate)
                    fee = qty * fill * fee_rate
                    cash += qty * fill - fee
                    pnl = (fill - entry_price) * qty - entry_fee - fee
                    realized_pnl += pnl
                    closed_trade_pnls.append(pnl)
                    trades.append(
                        {
                            "open_time": open_time[fill_index],
                            "side": "SELL",
                            "action": "EXIT_LONG_WEAK_BUY_SMB" if exit_signal else "EXIT_LONG_TIME_SMB",
                            "price": fill,
                            "qty": qty,
                            "fee": fee,
                            "trade_pnl": pnl,
                            "hold_bars": hold_bars,
                            "equity_after": cash,
                        }
                    )
                    position_qty = 0.0
                    position_side = None
                    entry_price = 0.0
                    entry_fee = 0.0
                    entry_bar_index = None
                    stop_price = None
                    strategy.set_long_open(symbol=run_cfg.symbol, is_open=False)
            elif position_side == "SHORT":
                exit_signal = any(decision.decision_type == StrategyDecisionType.EXIT_SHORT for decision in decisions)
                time_stop = hold_bars is not None and hold_bars >= reversal_max_hold_bars
                stop_triggered = stop_price is not None and high[signal_index] >= stop_price
                if exit_signal or time_stop or stop_triggered:
                    qty = abs(position_qty)
                    base_price = open_price[fill_index]
                    if stop_triggered and stop_price is not None:
                        base_price = max(base_price, stop_price)
                    fill = base_price * (1.0 + slippage_rate)
                    fee = qty * fill * fee_rate
                    cash -= qty * fill + fee
                    pnl = (entry_price - fill) * qty - entry_fee - fee
                    realized_pnl += pnl
                    closed_trade_pnls.append(pnl)
                    exit_action = (
                        "EXIT_SHORT_SIGNAL_REV_SMB"
                        if exit_signal
                        else ("EXIT_SHORT_STOP_REV_SMB" if stop_triggered else "EXIT_SHORT_TIME_REV_SMB")
                    )
                    trades.append(
                        {
                            "open_time": open_time[fill_index],
                            "side": "BUY",
                            "action": exit_action,
                            "price": fill,
                            "qty": qty,
                            "fee": fee,
                            "trade_pnl": pnl,
                            "hold_bars": hold_bars,
                            "stop_price": stop_price,
                            "equity_after": cash,
                        }
                    )
                    position_qty = 0.0
                    position_side = None
                    entry_price = 0.0
                    entry_fee = 0.0
                    entry_bar_index = None
                    stop_price = None
                    strategy.set_short_open(symbol=run_cfg.symbol, is_open=False)

        equity = cash + position_qty * close[fill_index]
        max_equity = max(max_equity, equity)
        if max_equity > 0:
            max_drawdown = max(max_drawdown, (max_equity - equity) / max_equity)
        equity_curve.append(
            {
                "open_time": open_time[fill_index],
                "close": close[fill_index],
                "equity": equity,
                "position_qty": position_qty,
                "position_side": position_side,
                "stop_price": stop_price,
            }
        )

    strategy.on_finish(StrategyContext(symbol=run_cfg.symbol, bars=prepared))
    final_equity = cash + position_qty * close[-1]
    net_pnl = final_equity - run_cfg.initial_capital
    wins = sum(1 for value in closed_trade_pnls if value > 0)
    gross_profit = sum(value for value in closed_trade_pnls if value > 0)
    gross_loss = abs(sum(value for value in closed_trade_pnls if value < 0))
    closed_trades = len(closed_trade_pnls)
    return _print_and_write_result(
        {
            "symbol": run_cfg.symbol,
            "interval": run_cfg.interval,
            "start_open_time": open_time[0],
            "end_open_time": open_time[-1],
            "config": {
                "avg_trade_size_window": strategy.config.avg_trade_size_window,
                "size_zscore_threshold": strategy.config.size_zscore_threshold,
                "min_taker_buy_ratio": strategy.config.min_taker_buy_ratio,
                "entry_confirm_buy_ratio_threshold": strategy.config.entry_confirm_buy_ratio_threshold,
                "close_to_high_threshold": strategy.config.close_to_high_threshold,
                "exit_buy_ratio_threshold": strategy.config.exit_buy_ratio_threshold,
                "max_hold_bars": max_hold_bars,
                "enable_failed_breakout_reversal": strategy.config.enable_failed_breakout_reversal,
                "reversal_trigger_buy_ratio_threshold": strategy.config.reversal_trigger_buy_ratio_threshold,
                "reversal_close_location_max": strategy.config.reversal_close_location_max,
                "reversal_exit_buy_ratio_threshold": strategy.config.reversal_exit_buy_ratio_threshold,
                "reversal_max_hold_bars": reversal_max_hold_bars,
                "reversal_stop_buffer_pct": reversal_stop_buffer_pct,
                "initial_capital": run_cfg.initial_capital,
                "fee_bps": run_cfg.fee_bps,
                "slippage_bps": run_cfg.slippage_bps,
                "max_leverage": run_cfg.max_leverage,
            },
            "summary": {
                "initial_capital": run_cfg.initial_capital,
                "final_equity": final_equity,
                "net_pnl": net_pnl,
                "return_pct": 0.0 if run_cfg.initial_capital == 0 else net_pnl / run_cfg.initial_capital,
                "max_drawdown": max_drawdown,
                "total_trades": len(trades),
                "closed_trades": closed_trades,
                "win_rate": 0.0 if closed_trades == 0 else wins / closed_trades,
                "gross_profit": gross_profit,
                "gross_loss": gross_loss,
                "profit_factor": None if gross_loss == 0 else gross_profit / gross_loss,
                "realized_pnl": realized_pnl,
                "final_position_qty": position_qty,
                "open_position_side": position_side,
            },
            "trades": trades,
            "equity_curve": equity_curve,
            "data_source": data_source,
            "data_source_htf": None,
        },
        run_cfg.output,
    )


__all__ = [
    "execute_rp_daily_breakout",
    "execute_rsi_threshold",
    "execute_liquidity_shock_reversion",
    "execute_taker_imbalance_absorption",
    "execute_liquidation_vacuum_reversion",
    "execute_smart_money_size_breakout",
]
