from cta_core.app.live_config import LiveRunConfig


def test_live_run_config_from_args() -> None:
    config = LiveRunConfig.from_argv(
        [
            "--strategy",
            "rp_daily_breakout",
            "--symbol",
            "BTCUSDT",
            "--interval",
            "1h",
            "--lookback-bars",
            "300",
            "--poll-seconds",
            "2",
            "--dry-run",
        ]
    )

    assert config.strategy_id == "rp_daily_breakout"
    assert config.symbol == "BTCUSDT"
    assert config.interval == "1h"
    assert config.lookback_bars == 300
    assert config.poll_seconds == 2
    assert config.dry_run is True
