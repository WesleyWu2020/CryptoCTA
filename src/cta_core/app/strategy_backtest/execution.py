from __future__ import annotations

import argparse

from cta_core.app.turtle_backtest import run_turtle_backtest, write_backtest_output
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
    result = run_turtle_backtest(
        bars=bars,
        bars_htf=None,
        symbol=run_cfg.symbol,
        interval=run_cfg.interval,
        config=strategy_config,
    )

    result["data_source"] = data_source
    result["data_source_htf"] = None

    path = write_backtest_output(result, run_cfg.output)
    print(path)
    print(result["summary"])
    return 0


__all__ = ["execute_rp_daily_breakout"]
