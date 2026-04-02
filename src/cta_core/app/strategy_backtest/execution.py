from __future__ import annotations

import argparse

from cta_core.app.turtle_backtest import TurtleConfig, run_turtle_backtest, write_backtest_output
from cta_core.config.run_config import RunConfig
from cta_core.data.market_data_store import utc_ms
from cta_core.strategy_runtime.strategies.rp_daily_breakout import RPDailyBreakoutStrategy

from .data_source import load_or_fetch


def execute_rp_daily_breakout(args: argparse.Namespace) -> int:
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
        regime_ema_window=getattr(args, "regime_ema_window", 30),
        regime_min_slope=getattr(args, "regime_min_slope", 0.002),
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

    path = write_backtest_output(result, run_cfg.output)
    print(path)
    print(result["summary"])
    return 0


__all__ = ["execute_rp_daily_breakout"]
