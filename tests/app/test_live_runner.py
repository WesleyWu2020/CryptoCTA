from decimal import Decimal

import pytest
import polars as pl

from cta_core.app import live_runner
from cta_core.app.live_config import LiveRunConfig
from cta_core.app.live_runner import compute_runtime_alerts
from cta_core.app.live_state import LiveRuntimeState
from cta_core.events import Side
from cta_core.risk import RiskContext, RiskEngine
from cta_core.strategy_runtime import StrategyContext, StrategyDecision, StrategyDecisionType
from cta_core.strategy_runtime.strategies import RPDailyBreakoutConfig, RPDailyBreakoutStrategy


class DummyAdapter:
    def __init__(self) -> None:
        self.submitted: list[dict[str, object]] = []

    def submit_order(self, *, intent, ts_ms: int) -> dict[str, object]:
        record = {"intent": intent, "ts_ms": ts_ms}
        self.submitted.append(record)
        return record


class HistoryAwareStrategy:
    strategy_id = "history_aware"

    def __init__(self) -> None:
        self.observed_heights: list[int] = []

    def prepare_features(self, bars: pl.DataFrame, bars_htf: pl.DataFrame | None = None) -> pl.DataFrame:
        return bars

    def on_start(self, context) -> None:
        return None

    def on_bar(self, context) -> list[StrategyDecision]:
        self.observed_heights.append(context.bars.height)
        if context.bars.height >= 2:
            return [
                StrategyDecision(
                    decision_type=StrategyDecisionType.ENTER_LONG,
                    size=Decimal("1"),
                    reason="history_seen",
                )
            ]
        return []

    def on_finish(self, context) -> None:
        return None


class EnterLongStrategy:
    strategy_id = "enter_long"

    def prepare_features(self, bars: pl.DataFrame, bars_htf: pl.DataFrame | None = None) -> pl.DataFrame:
        return bars

    def on_start(self, context) -> None:
        return None

    def on_bar(self, context) -> list[StrategyDecision]:
        return [
            StrategyDecision(
                decision_type=StrategyDecisionType.ENTER_LONG,
                size=Decimal("1"),
                reason="enter",
            )
        ]

    def on_finish(self, context) -> None:
        return None


class TwoEnterLongStrategy:
    strategy_id = "two_enter_long"

    def prepare_features(self, bars: pl.DataFrame, bars_htf: pl.DataFrame | None = None) -> pl.DataFrame:
        return bars

    def on_start(self, context) -> None:
        return None

    def on_bar(self, context) -> list[StrategyDecision]:
        return [
            StrategyDecision(
                decision_type=StrategyDecisionType.ENTER_LONG,
                size=Decimal("0.5"),
                reason="first",
            ),
            StrategyDecision(
                decision_type=StrategyDecisionType.ENTER_LONG,
                size=Decimal("0.5"),
                reason="second",
            ),
        ]

    def on_finish(self, context) -> None:
        return None


def test_decision_to_intent_maps_enter_long() -> None:
    decision = StrategyDecision(decision_type=StrategyDecisionType.ENTER_LONG, size=Decimal("0.5"))

    intent = live_runner.decision_to_intent(
        "rp_daily_breakout",
        "BTCUSDT",
        decision,
        latest_price=Decimal("100"),
        equity=Decimal("1000"),
        max_leverage=Decimal("2"),
        fee_bps=Decimal("10"),
    )

    assert intent is not None
    assert intent.strategy_id == "rp_daily_breakout"
    assert intent.symbol == "BTCUSDT"
    assert intent.side is Side.BUY
    expected_qty = Decimal("1000") * Decimal("2") * Decimal("0.5") / (Decimal("100") * Decimal("1.001"))
    assert float(intent.quantity) == pytest.approx(float(expected_qty), rel=1e-12)
    assert intent.order_type == "MARKET"


def test_decision_to_intent_enter_long_requires_sizing_context() -> None:
    decision = StrategyDecision(decision_type=StrategyDecisionType.ENTER_LONG, size=Decimal("0.5"))

    with pytest.raises(ValueError, match="requires latest_price and equity"):
        live_runner.decision_to_intent("rp_daily_breakout", "BTCUSDT", decision)


def test_decision_to_intent_maps_exit_long_with_explicit_size() -> None:
    decision = StrategyDecision(decision_type=StrategyDecisionType.EXIT_LONG, size=Decimal("0.25"))

    intent = live_runner.decision_to_intent("rp_daily_breakout", "BTCUSDT", decision)

    assert intent is not None
    assert intent.strategy_id == "rp_daily_breakout"
    assert intent.symbol == "BTCUSDT"
    assert intent.side is Side.SELL
    assert intent.quantity == Decimal("0.25")
    assert intent.order_type == "MARKET"


def test_decision_to_intent_raises_for_exit_long_without_quantity_context() -> None:
    decision = StrategyDecision(decision_type=StrategyDecisionType.EXIT_LONG, size=Decimal("0"))

    with pytest.raises(ValueError, match="cannot map EXIT_LONG without positive size/position qty"):
        live_runner.decision_to_intent("rp_daily_breakout", "BTCUSDT", decision)


def test_decision_to_intent_maps_exit_long_with_position_qty_fallback() -> None:
    decision = StrategyDecision(decision_type=StrategyDecisionType.EXIT_LONG, size=Decimal("0"))

    intent = live_runner.decision_to_intent(
        "rp_daily_breakout",
        "BTCUSDT",
        decision,
        position_qty=Decimal("1.5"),
    )

    assert intent is not None
    assert intent.strategy_id == "rp_daily_breakout"
    assert intent.symbol == "BTCUSDT"
    assert intent.side is Side.SELL
    assert intent.quantity == Decimal("1.5")
    assert intent.order_type == "MARKET"


def test_decision_to_intent_raises_for_exit_long_with_negative_position_qty_fallback() -> None:
    decision = StrategyDecision(decision_type=StrategyDecisionType.EXIT_LONG, size=Decimal("0"))

    with pytest.raises(ValueError, match="cannot map EXIT_LONG without positive size/position qty"):
        live_runner.decision_to_intent(
            "rp_daily_breakout",
            "BTCUSDT",
            decision,
            position_qty=Decimal("-1.5"),
        )


def test_decision_to_intent_returns_none_for_unsupported_decision_type() -> None:
    decision = StrategyDecision(decision_type=StrategyDecisionType.HOLD)

    intent = live_runner.decision_to_intent("rp_daily_breakout", "BTCUSDT", decision)

    assert intent is None


def test_compute_runtime_alerts_raises_submit_error_alert() -> None:
    alerts = compute_runtime_alerts(
        peak_equity=1000.0,
        current_equity=920.0,
        submit_attempts=100,
        submit_errors=5,
        ws_disconnects=0,
    )

    assert "submit_error_spike" in alerts
    assert "drawdown_breach" in alerts


def test_run_once_dry_run_does_not_submit() -> None:
    bars = pl.DataFrame(
        {
            "open_time": [3_000, 1_000, 2_000],
            "close": [13.0, 10.0, 12.0],
        }
    )
    strategy = RPDailyBreakoutStrategy(
        RPDailyBreakoutConfig(rp_window=2, entry_confirmations=2, exit_confirmations=2, quantity=Decimal("1"))
    )
    adapter = DummyAdapter()

    result = live_runner.run_once(
        strategy=strategy,
        adapter=adapter,
        bars=bars,
        symbol="BTCUSDT",
        dry_run=True,
    )

    assert adapter.submitted == []
    assert result["decisions_count"] == 1
    assert result["submit_attempts_count"] == 0
    assert result["submit_errors_count"] == 0
    assert result["submit_count"] == 0
    assert result["submitted_intents"] == []
    assert result["latest_open_time"] == 3_000
    assert result["dry_run"] is True


def test_run_once_uses_full_history_context() -> None:
    bars = pl.DataFrame(
        {
            "open_time": [2_000, 1_000],
            "close": [12.0, 10.0],
        }
    )
    strategy = HistoryAwareStrategy()
    adapter = DummyAdapter()

    result = live_runner.run_once(
        strategy=strategy,
        adapter=adapter,
        bars=bars,
        symbol="BTCUSDT",
        dry_run=True,
    )

    assert strategy.observed_heights == [2]
    assert result["decisions_count"] == 1
    assert result["submit_count"] == 0
    assert adapter.submitted == []


def test_sync_strategy_position_state_sets_open_when_exchange_has_long() -> None:
    strategy = RPDailyBreakoutStrategy(
        RPDailyBreakoutConfig(rp_window=2, entry_confirmations=2, exit_confirmations=2, quantity=Decimal("1"))
    )
    symbol = "BTCUSDT"
    bars = pl.DataFrame(
        {
            "open_time": [1_000, 2_000, 3_000],
            "close": [10.0, 9.0, 8.0],
        }
    )

    live_runner.sync_strategy_position_state(strategy, symbol, position_qty=Decimal("0.2"))

    prepared_bars = strategy.prepare_features(bars.sort("open_time"))
    decisions = strategy.on_bar(StrategyContext(symbol=symbol, bars=prepared_bars))

    assert len(decisions) == 1
    assert decisions[0].decision_type is StrategyDecisionType.EXIT_LONG


def test_sync_strategy_position_state_does_not_set_open_when_exchange_is_net_short() -> None:
    strategy = RPDailyBreakoutStrategy(
        RPDailyBreakoutConfig(rp_window=2, entry_confirmations=2, exit_confirmations=2, quantity=Decimal("1"))
    )
    symbol = "BTCUSDT"
    bars = pl.DataFrame(
        {
            "open_time": [1_000, 2_000, 3_000],
            "close": [10.0, 9.0, 8.0],
        }
    )

    live_runner.sync_strategy_position_state(strategy, symbol, position_qty=Decimal("-0.2"))

    prepared_bars = strategy.prepare_features(bars.sort("open_time"))
    decisions = strategy.on_bar(StrategyContext(symbol=symbol, bars=prepared_bars))

    assert decisions == []


def test_run_once_non_dry_submit_uses_latest_open_time() -> None:
    bars = pl.DataFrame(
        {
            "open_time": [2_000, 3_000, 1_000],
            "close": [12.0, 13.0, 10.0],
        }
    )
    strategy = EnterLongStrategy()
    adapter = DummyAdapter()

    result = live_runner.run_once(
        strategy=strategy,
        adapter=adapter,
        bars=bars,
        symbol="BTCUSDT",
        dry_run=False,
        equity=Decimal("1000"),
        max_leverage=Decimal("1"),
        risk_engine=RiskEngine(max_daily_loss=Decimal("1000"), max_symbol_notional_ratio=Decimal("2")),
    )

    assert result["decisions_count"] == 1
    assert result["submit_attempts_count"] == 1
    assert result["submit_errors_count"] == 0
    assert result["submit_count"] == 1
    assert len(adapter.submitted) == 1
    assert adapter.submitted[0]["ts_ms"] == 3_000
    intent = adapter.submitted[0]["intent"]
    assert float(intent.quantity) == pytest.approx(1000.0 / 13.0, rel=1e-12)


def test_run_once_non_dry_requires_risk_engine() -> None:
    bars = pl.DataFrame(
        {
            "open_time": [2_000, 3_000, 1_000],
            "close": [12.0, 13.0, 10.0],
        }
    )
    strategy = EnterLongStrategy()
    adapter = DummyAdapter()

    with pytest.raises(ValueError, match="risk_engine is required"):
        live_runner.run_once(
            strategy=strategy,
            adapter=adapter,
            bars=bars,
            symbol="BTCUSDT",
            dry_run=False,
            equity=Decimal("1000"),
            max_leverage=Decimal("1"),
        )


def test_run_once_non_dry_skips_submit_when_risk_rejects() -> None:
    bars = pl.DataFrame(
        {
            "open_time": [2_000, 3_000, 1_000],
            "close": [12.0, 13.0, 10.0],
        }
    )
    strategy = EnterLongStrategy()
    adapter = DummyAdapter()

    result = live_runner.run_once(
        strategy=strategy,
        adapter=adapter,
        bars=bars,
        symbol="BTCUSDT",
        dry_run=False,
        equity=Decimal("1000"),
        max_leverage=Decimal("1"),
        risk_engine=RiskEngine(max_daily_loss=Decimal("1000"), max_symbol_notional_ratio=Decimal("0.1")),
    )

    assert result["decisions_count"] == 1
    assert result["submit_attempts_count"] == 0
    assert result["submit_errors_count"] == 0
    assert result["submit_count"] == 0
    assert adapter.submitted == []
    assert len(result["risk_rejections"]) == 1
    assert result["risk_rejections"][0]["rule"] == "symbol_risk_budget"


def test_run_once_non_dry_records_submit_errors_and_continues() -> None:
    bars = pl.DataFrame(
        {
            "open_time": [2_000, 3_000, 1_000],
            "close": [12.0, 13.0, 10.0],
        }
    )
    strategy = TwoEnterLongStrategy()

    class FlakyAdapter:
        def __init__(self) -> None:
            self.calls = 0
            self.submitted: list[dict[str, object]] = []

        def submit_order(self, *, intent, ts_ms: int) -> dict[str, object]:
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("exchange unavailable")
            record = {"intent": intent, "ts_ms": ts_ms}
            self.submitted.append(record)
            return record

    adapter = FlakyAdapter()

    result = live_runner.run_once(
        strategy=strategy,
        adapter=adapter,
        bars=bars,
        symbol="BTCUSDT",
        dry_run=False,
        equity=Decimal("1000"),
        max_leverage=Decimal("1"),
        risk_engine=RiskEngine(max_daily_loss=Decimal("1000"), max_symbol_notional_ratio=Decimal("2")),
    )

    assert result["decisions_count"] == 2
    assert result["submit_attempts_count"] == 2
    assert result["submit_errors_count"] == 1
    assert result["submit_count"] == 1
    assert len(result["submit_errors"]) == 1
    assert result["submit_errors"][0]["decision_type"] == StrategyDecisionType.ENTER_LONG.value
    assert "exchange unavailable" in result["submit_errors"][0]["error"]
    assert len(adapter.submitted) == 1


def test_check_risk_rejects_when_symbol_budget_exceeded() -> None:
    engine = RiskEngine(max_daily_loss=Decimal("100"))
    ctx = RiskContext(
        symbol="BTCUSDT",
        order_notional=Decimal("300"),
        equity=Decimal("1000"),
        day_pnl=Decimal("0"),
        losing_streak=0,
        symbol_notional=Decimal("150"),
    )

    result = live_runner.check_risk(engine, ctx)

    assert result.allowed is False
    assert result.rule == "symbol_risk_budget"
    assert "exceeds" in result.detail


def test_validate_live_mode_requires_credentials_without_dry_run() -> None:
    with pytest.raises(ValueError, match="api_key and api_secret"):
        live_runner.validate_live_mode(dry_run=False, api_key="", api_secret="")


def test_main_dry_run_delegates_to_run_live_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[LiveRunConfig] = []

    def fake_run_live_loop(config: LiveRunConfig) -> int:
        calls.append(config)
        return 0

    monkeypatch.setattr(live_runner, "run_live_loop", fake_run_live_loop)

    result = live_runner.main(["--strategy", "rp_daily_breakout", "--dry-run"])

    assert result == 0
    assert len(calls) == 1
    assert calls[0].dry_run is True


def test_main_non_dry_run_delegates_to_run_live_loop_with_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[LiveRunConfig] = []

    def fake_run_live_loop(config: LiveRunConfig) -> int:
        calls.append(config)
        return 0

    monkeypatch.setattr(live_runner, "run_live_loop", fake_run_live_loop)

    result = live_runner.main(
        [
            "--strategy",
            "rp_daily_breakout",
            "--api-key",
            "key",
            "--api-secret",
            "secret",
        ]
    )

    assert result == 0
    assert len(calls) == 1
    assert calls[0].api_key == "key"
    assert calls[0].api_secret == "secret"


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
            "--state-path",
            "artifacts/live_state/custom.json",
            "--max-daily-loss",
            "750",
            "--max-losing-streak",
            "4",
            "--max-symbol-notional-ratio",
            "0.6",
            "--max-leverage",
            "2",
            "--fee-bps",
            "7",
            "--max-cycles",
            "5",
            "--dry-run",
        ]
    )

    assert config.strategy_id == "rp_daily_breakout"
    assert config.symbol == "BTCUSDT"
    assert config.interval == "1h"
    assert config.lookback_bars == 300
    assert config.poll_seconds == 2
    assert config.state_path == "artifacts/live_state/custom.json"
    assert config.max_daily_loss == Decimal("750")
    assert config.max_losing_streak == 4
    assert config.max_symbol_notional_ratio == Decimal("0.6")
    assert config.max_leverage == Decimal("2")
    assert config.fee_bps == Decimal("7")
    assert config.max_cycles == 5
    assert config.dry_run is True


def test_run_live_loop_uses_full_history_context_when_new_closed_bar_arrives(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_once_calls: list[dict[str, object]] = []
    saved_states: list[LiveRuntimeState] = []

    class FakeStrategy:
        strategy_id = "rp_daily_breakout"
        started = False
        finished = False

        def prepare_features(self, bars: pl.DataFrame, bars_htf: pl.DataFrame | None = None) -> pl.DataFrame:
            return bars

        def on_start(self, context) -> None:
            self.started = True

        def on_bar(self, context) -> list[StrategyDecision]:
            return []

        def on_finish(self, context) -> None:
            self.finished = True

    class FakeAdapter:
        def fetch_account_snapshot(self, symbol: str, now_ms: int) -> object:
            assert symbol == "BTCUSDT"
            assert now_ms == 3_100
            return type(
                "Snapshot",
                (),
                {
                    "position_qty": Decimal("0"),
                    "equity": Decimal("1000"),
                    "day_pnl": Decimal("0"),
                    "losing_streak": 0,
                    "symbol_notional": Decimal("0"),
                },
            )()

    class FakeMarketClient:
        pass

    strategy = FakeStrategy()
    snapshot_calls: list[tuple[str, int]] = []

    def fake_build_strategy(strategy_id: str) -> FakeStrategy:
        assert strategy_id == "rp_daily_breakout"
        return strategy

    def fake_bootstrap_live_runner(*, api_key: str, api_secret: str) -> FakeAdapter:
        assert api_key == "key"
        assert api_secret == "secret"
        return FakeAdapter()

    def fake_fetch_closed_bars(*, client, symbol: str, interval: str, lookback_bars: int, now_ms: int) -> pl.DataFrame:
        assert isinstance(client, FakeMarketClient)
        assert symbol == "BTCUSDT"
        assert interval == "1h"
        assert lookback_bars == 300
        assert now_ms == 3_100
        return pl.DataFrame(
            {
                "open_time": [1_000, 2_000],
                "close_time": [1_999, 2_999],
                "close": [10.0, 11.0],
            }
        )

    def fake_run_once(**kwargs) -> dict[str, object]:
        run_once_calls.append(kwargs)
        bars = kwargs["bars"]
        assert isinstance(bars, pl.DataFrame)
        assert bars.get_column("open_time").to_list() == [1_000, 2_000]
        return {
            "decisions_count": 0,
            "latest_open_time": 2_000,
            "risk_rejections": [],
            "submit_count": 1,
        }

    def fake_load_live_state(path) -> LiveRuntimeState:
        return LiveRuntimeState(last_processed_open_time=1_000)

    def fake_save_live_state(path, state: LiveRuntimeState) -> None:
        saved_states.append(state)

    original_fetch_account_snapshot = FakeAdapter.fetch_account_snapshot

    def wrapped_fetch_account_snapshot(self, symbol: str, now_ms: int) -> object:
        snapshot_calls.append((symbol, now_ms))
        return original_fetch_account_snapshot(self, symbol, now_ms)

    FakeAdapter.fetch_account_snapshot = wrapped_fetch_account_snapshot

    monkeypatch.setattr(live_runner, "build_strategy", fake_build_strategy)
    monkeypatch.setattr(live_runner, "bootstrap_live_runner", fake_bootstrap_live_runner)
    monkeypatch.setattr(live_runner, "fetch_closed_bars", fake_fetch_closed_bars)
    monkeypatch.setattr(live_runner, "run_once", fake_run_once)
    monkeypatch.setattr(live_runner, "load_live_state", fake_load_live_state)
    monkeypatch.setattr(live_runner, "save_live_state", fake_save_live_state)
    monkeypatch.setattr(live_runner, "BinanceUMClient", FakeMarketClient)

    result = live_runner.run_live_loop(
        LiveRunConfig.from_argv(
            [
                "--strategy",
                "rp_daily_breakout",
                "--api-key",
                "key",
                "--api-secret",
                "secret",
                "--max-cycles",
                "1",
            ]
        ),
        sleep_fn=lambda _seconds: None,
        now_ms_fn=lambda: 3_100,
    )

    assert result == 0
    assert len(run_once_calls) == 1
    assert run_once_calls[0]["bars"].get_column("open_time").to_list() == [1_000, 2_000]
    assert snapshot_calls == [("BTCUSDT", 3_100)]
    assert saved_states[-1].last_processed_open_time == 2_000
    assert saved_states[-1].last_submit_ts_ms == 2_000
    assert strategy.started is True
    assert strategy.finished is True


def test_run_live_loop_dry_run_uses_local_snapshot_without_bootstrap_or_account_fetch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_once_calls: list[dict[str, object]] = []
    saved_states: list[LiveRuntimeState] = []

    class FakeStrategy:
        strategy_id = "rp_daily_breakout"

        def prepare_features(self, bars: pl.DataFrame, bars_htf: pl.DataFrame | None = None) -> pl.DataFrame:
            return bars

        def on_start(self, context) -> None:
            return None

        def on_bar(self, context) -> list[StrategyDecision]:
            return []

        def on_finish(self, context) -> None:
            return None

    class FakeMarketClient:
        pass

    def fake_build_strategy(strategy_id: str) -> FakeStrategy:
        assert strategy_id == "rp_daily_breakout"
        return FakeStrategy()

    def fake_bootstrap_live_runner(*, api_key: str, api_secret: str) -> object:
        raise AssertionError("bootstrap_live_runner should not be called in dry-run mode")

    def fake_fetch_closed_bars(*, client, symbol: str, interval: str, lookback_bars: int, now_ms: int) -> pl.DataFrame:
        assert isinstance(client, FakeMarketClient)
        assert symbol == "BTCUSDT"
        assert interval == "1h"
        assert lookback_bars == 300
        assert now_ms == 3_100
        return pl.DataFrame(
            {
                "open_time": [1_000, 2_000],
                "close_time": [1_999, 2_999],
                "close": [10.0, 11.0],
            }
        )

    def fake_run_once(**kwargs) -> dict[str, object]:
        run_once_calls.append(kwargs)
        assert kwargs["dry_run"] is True
        assert kwargs["adapter"] is None
        assert kwargs["position_qty"] == Decimal("0")
        assert kwargs["equity"] == Decimal("0")
        assert kwargs["day_pnl"] == Decimal("0")
        assert kwargs["losing_streak"] == 0
        assert kwargs["symbol_notional"] == Decimal("0")
        assert kwargs["bars"].get_column("open_time").to_list() == [1_000, 2_000]
        return {
            "decisions_count": 0,
            "latest_open_time": 2_000,
            "risk_rejections": [],
            "submit_count": 0,
        }

    def fake_load_live_state(path) -> LiveRuntimeState:
        return LiveRuntimeState(last_processed_open_time=1_000)

    def fake_save_live_state(path, state: LiveRuntimeState) -> None:
        saved_states.append(state)

    monkeypatch.setattr(live_runner, "build_strategy", fake_build_strategy)
    monkeypatch.setattr(live_runner, "bootstrap_live_runner", fake_bootstrap_live_runner)
    monkeypatch.setattr(live_runner, "fetch_closed_bars", fake_fetch_closed_bars)
    monkeypatch.setattr(live_runner, "run_once", fake_run_once)
    monkeypatch.setattr(live_runner, "load_live_state", fake_load_live_state)
    monkeypatch.setattr(live_runner, "save_live_state", fake_save_live_state)
    monkeypatch.setattr(live_runner, "BinanceUMClient", FakeMarketClient)

    result = live_runner.run_live_loop(
        LiveRunConfig.from_argv(["--strategy", "rp_daily_breakout", "--dry-run", "--max-cycles", "1"]),
        sleep_fn=lambda _seconds: None,
        now_ms_fn=lambda: 3_100,
    )

    assert result == 0
    assert len(run_once_calls) == 1
    assert saved_states[-1].last_processed_open_time == 2_000
    assert saved_states[-1].last_submit_ts_ms is None


def test_run_live_loop_does_not_reprocess_same_open_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_once_calls: list[dict[str, object]] = []

    class FakeStrategy:
        strategy_id = "rp_daily_breakout"

        def prepare_features(self, bars: pl.DataFrame, bars_htf: pl.DataFrame | None = None) -> pl.DataFrame:
            return bars

        def on_start(self, context) -> None:
            return None

        def on_bar(self, context) -> list[StrategyDecision]:
            return []

        def on_finish(self, context) -> None:
            return None

    class FakeMarketClient:
        pass

    def fake_build_strategy(strategy_id: str) -> FakeStrategy:
        assert strategy_id == "rp_daily_breakout"
        return FakeStrategy()

    def fake_fetch_closed_bars(*, client, symbol: str, interval: str, lookback_bars: int, now_ms: int) -> pl.DataFrame:
        assert isinstance(client, FakeMarketClient)
        return pl.DataFrame(
            {
                "open_time": [1_000, 2_000],
                "close_time": [1_999, 2_999],
                "close": [10.0, 11.0],
            }
        )

    def fake_run_once(**kwargs) -> dict[str, object]:
        run_once_calls.append(kwargs)
        return {
            "decisions_count": 0,
            "latest_open_time": 2_000,
            "risk_rejections": [],
            "submit_count": 0,
        }

    def fake_load_live_state(path) -> LiveRuntimeState:
        return LiveRuntimeState(last_processed_open_time=1_000)

    monkeypatch.setattr(live_runner, "build_strategy", fake_build_strategy)
    monkeypatch.setattr(live_runner, "fetch_closed_bars", fake_fetch_closed_bars)
    monkeypatch.setattr(live_runner, "run_once", fake_run_once)
    monkeypatch.setattr(live_runner, "load_live_state", fake_load_live_state)
    monkeypatch.setattr(live_runner, "save_live_state", lambda path, state: None)
    monkeypatch.setattr(live_runner, "BinanceUMClient", FakeMarketClient)

    result = live_runner.run_live_loop(
        LiveRunConfig.from_argv(["--strategy", "rp_daily_breakout", "--dry-run", "--max-cycles", "2"]),
        sleep_fn=lambda _seconds: None,
        now_ms_fn=lambda: 3_100,
    )

    assert result == 0
    assert len(run_once_calls) == 1


def test_run_live_loop_reprocesses_only_newly_advanced_open_times(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_once_calls: list[dict[str, object]] = []
    saved_states: list[LiveRuntimeState] = []
    cycle_bars = [
        pl.DataFrame(
            {
                "open_time": [1_000, 2_000],
                "close_time": [1_999, 2_999],
                "close": [10.0, 11.0],
            }
        ),
        pl.DataFrame(
            {
                "open_time": [1_000, 2_000],
                "close_time": [1_999, 2_999],
                "close": [10.0, 11.0],
            }
        ),
        pl.DataFrame(
            {
                "open_time": [1_000, 2_000, 3_000],
                "close_time": [1_999, 2_999, 3_999],
                "close": [10.0, 11.0, 12.0],
            }
        ),
    ]

    class FakeStrategy:
        strategy_id = "rp_daily_breakout"

        def prepare_features(self, bars: pl.DataFrame, bars_htf: pl.DataFrame | None = None) -> pl.DataFrame:
            return bars

        def on_start(self, context) -> None:
            return None

        def on_bar(self, context) -> list[StrategyDecision]:
            return []

        def on_finish(self, context) -> None:
            return None

    class FakeMarketClient:
        pass

    def fake_build_strategy(strategy_id: str) -> FakeStrategy:
        assert strategy_id == "rp_daily_breakout"
        return FakeStrategy()

    def fake_fetch_closed_bars(*, client, symbol: str, interval: str, lookback_bars: int, now_ms: int) -> pl.DataFrame:
        assert isinstance(client, FakeMarketClient)
        return cycle_bars.pop(0)

    def fake_run_once(**kwargs) -> dict[str, object]:
        run_once_calls.append(kwargs)
        latest_open_time = kwargs["bars"].get_column("open_time").max()
        return {
            "decisions_count": 0,
            "latest_open_time": latest_open_time,
            "risk_rejections": [],
            "submit_count": 0,
        }

    def fake_load_live_state(path) -> LiveRuntimeState:
        return LiveRuntimeState(last_processed_open_time=1_000)

    def fake_save_live_state(path, state: LiveRuntimeState) -> None:
        saved_states.append(state)

    monkeypatch.setattr(live_runner, "build_strategy", fake_build_strategy)
    monkeypatch.setattr(live_runner, "fetch_closed_bars", fake_fetch_closed_bars)
    monkeypatch.setattr(live_runner, "run_once", fake_run_once)
    monkeypatch.setattr(live_runner, "load_live_state", fake_load_live_state)
    monkeypatch.setattr(live_runner, "save_live_state", fake_save_live_state)
    monkeypatch.setattr(live_runner, "BinanceUMClient", FakeMarketClient)

    result = live_runner.run_live_loop(
        LiveRunConfig.from_argv(["--strategy", "rp_daily_breakout", "--dry-run", "--max-cycles", "3"]),
        sleep_fn=lambda _seconds: None,
        now_ms_fn=lambda: 3_100,
    )

    assert result == 0
    assert len(run_once_calls) == 2
    assert [call["bars"].get_column("open_time").to_list() for call in run_once_calls] == [
        [1_000, 2_000],
        [1_000, 2_000, 3_000],
    ]
    assert [state.last_processed_open_time for state in saved_states] == [2_000, 3_000]


def test_run_live_loop_persists_new_bar_checkpoint_when_run_once_reports_stale_open_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_once_calls: list[dict[str, object]] = []
    saved_states: list[LiveRuntimeState] = []
    cycle_bars = [
        pl.DataFrame(
            {
                "open_time": [1_000, 2_000],
                "close_time": [1_999, 2_999],
                "close": [10.0, 11.0],
            }
        ),
        pl.DataFrame(
            {
                "open_time": [1_000, 2_000, 3_000],
                "close_time": [1_999, 2_999, 3_999],
                "close": [10.0, 11.0, 12.0],
            }
        ),
        pl.DataFrame(
            {
                "open_time": [1_000, 2_000, 3_000],
                "close_time": [1_999, 2_999, 3_999],
                "close": [10.0, 11.0, 12.0],
            }
        ),
        pl.DataFrame(
            {
                "open_time": [1_000, 2_000, 3_000, 4_000],
                "close_time": [1_999, 2_999, 3_999, 4_999],
                "close": [10.0, 11.0, 12.0, 13.0],
            }
        ),
    ]
    reported_open_times = [2_000, 2_000, 4_000]

    class FakeStrategy:
        strategy_id = "rp_daily_breakout"

        def prepare_features(self, bars: pl.DataFrame, bars_htf: pl.DataFrame | None = None) -> pl.DataFrame:
            return bars

        def on_start(self, context) -> None:
            return None

        def on_bar(self, context) -> list[StrategyDecision]:
            return []

        def on_finish(self, context) -> None:
            return None

    class FakeMarketClient:
        pass

    def fake_build_strategy(strategy_id: str) -> FakeStrategy:
        assert strategy_id == "rp_daily_breakout"
        return FakeStrategy()

    def fake_fetch_closed_bars(*, client, symbol: str, interval: str, lookback_bars: int, now_ms: int) -> pl.DataFrame:
        assert isinstance(client, FakeMarketClient)
        return cycle_bars.pop(0)

    def fake_run_once(**kwargs) -> dict[str, object]:
        run_once_calls.append(kwargs)
        return {
            "decisions_count": 0,
            "latest_open_time": reported_open_times.pop(0),
            "risk_rejections": [],
            "submit_count": 0,
        }

    def fake_load_live_state(path) -> LiveRuntimeState:
        return LiveRuntimeState(last_processed_open_time=1_000)

    def fake_save_live_state(path, state: LiveRuntimeState) -> None:
        saved_states.append(state)

    monkeypatch.setattr(live_runner, "build_strategy", fake_build_strategy)
    monkeypatch.setattr(live_runner, "fetch_closed_bars", fake_fetch_closed_bars)
    monkeypatch.setattr(live_runner, "run_once", fake_run_once)
    monkeypatch.setattr(live_runner, "load_live_state", fake_load_live_state)
    monkeypatch.setattr(live_runner, "save_live_state", fake_save_live_state)
    monkeypatch.setattr(live_runner, "BinanceUMClient", FakeMarketClient)

    result = live_runner.run_live_loop(
        LiveRunConfig.from_argv(["--strategy", "rp_daily_breakout", "--dry-run", "--max-cycles", "4"]),
        sleep_fn=lambda _seconds: None,
        now_ms_fn=lambda: 3_100,
    )

    assert result == 0
    assert len(run_once_calls) == 3
    assert [call["bars"].get_column("open_time").to_list() for call in run_once_calls] == [
        [1_000, 2_000],
        [1_000, 2_000, 3_000],
        [1_000, 2_000, 3_000, 4_000],
    ]
    assert [state.last_processed_open_time for state in saved_states] == [2_000, 3_000, 4_000]


def test_run_live_loop_does_not_advance_checkpoint_when_submit_errors_occur(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    saved_states: list[LiveRuntimeState] = []

    class FakeStrategy:
        strategy_id = "rp_daily_breakout"

        def prepare_features(self, bars: pl.DataFrame, bars_htf: pl.DataFrame | None = None) -> pl.DataFrame:
            return bars

        def on_start(self, context) -> None:
            return None

        def on_bar(self, context) -> list[StrategyDecision]:
            return []

        def on_finish(self, context) -> None:
            return None

    class FakeMarketClient:
        pass

    def fake_build_strategy(strategy_id: str) -> FakeStrategy:
        assert strategy_id == "rp_daily_breakout"
        return FakeStrategy()

    def fake_fetch_closed_bars(*, client, symbol: str, interval: str, lookback_bars: int, now_ms: int) -> pl.DataFrame:
        assert isinstance(client, FakeMarketClient)
        return pl.DataFrame(
            {
                "open_time": [1_000, 2_000, 3_000],
                "close_time": [1_999, 2_999, 3_999],
                "close": [10.0, 11.0, 12.0],
            }
        )

    def fake_run_once(**kwargs) -> dict[str, object]:
        return {
            "decisions_count": 2,
            "latest_open_time": 3_000,
            "risk_rejections": [],
            "submit_attempts_count": 2,
            "submit_errors_count": 1,
            "submit_count": 1,
        }

    def fake_load_live_state(path) -> LiveRuntimeState:
        return LiveRuntimeState(last_processed_open_time=2_000, last_submit_ts_ms=2_000)

    def fake_save_live_state(path, state: LiveRuntimeState) -> None:
        saved_states.append(state)

    monkeypatch.setattr(live_runner, "build_strategy", fake_build_strategy)
    monkeypatch.setattr(live_runner, "fetch_closed_bars", fake_fetch_closed_bars)
    monkeypatch.setattr(live_runner, "run_once", fake_run_once)
    monkeypatch.setattr(live_runner, "load_live_state", fake_load_live_state)
    monkeypatch.setattr(live_runner, "save_live_state", fake_save_live_state)
    monkeypatch.setattr(live_runner, "BinanceUMClient", FakeMarketClient)

    result = live_runner.run_live_loop(
        LiveRunConfig.from_argv(["--strategy", "rp_daily_breakout", "--dry-run", "--max-cycles", "1"]),
        sleep_fn=lambda _seconds: None,
        now_ms_fn=lambda: 3_100,
    )

    assert result == 0
    assert saved_states[-1] == LiveRuntimeState(last_processed_open_time=2_000, last_submit_ts_ms=2_000)


def test_run_live_loop_does_not_raise_submit_error_alert_from_risk_rejections(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class FakeStrategy:
        strategy_id = "rp_daily_breakout"

        def prepare_features(self, bars: pl.DataFrame, bars_htf: pl.DataFrame | None = None) -> pl.DataFrame:
            return bars

        def on_start(self, context) -> None:
            return None

        def on_bar(self, context) -> list[StrategyDecision]:
            return []

        def on_finish(self, context) -> None:
            return None

    class FakeMarketClient:
        pass

    def fake_build_strategy(strategy_id: str) -> FakeStrategy:
        assert strategy_id == "rp_daily_breakout"
        return FakeStrategy()

    def fake_fetch_closed_bars(*, client, symbol: str, interval: str, lookback_bars: int, now_ms: int) -> pl.DataFrame:
        assert isinstance(client, FakeMarketClient)
        return pl.DataFrame(
            {
                "open_time": [1_000, 2_000],
                "close_time": [1_999, 2_999],
                "close": [10.0, 11.0],
            }
        )

    def fake_run_once(**kwargs) -> dict[str, object]:
        return {
            "decisions_count": 100,
            "latest_open_time": 2_000,
            "risk_rejections": [{"rule": "symbol_risk_budget"} for _ in range(5)],
            "submit_attempts_count": 0,
            "submit_errors_count": 0,
            "submit_count": 0,
        }

    def fake_load_live_state(path) -> LiveRuntimeState:
        return LiveRuntimeState(last_processed_open_time=1_000)

    monkeypatch.setattr(live_runner, "build_strategy", fake_build_strategy)
    monkeypatch.setattr(live_runner, "fetch_closed_bars", fake_fetch_closed_bars)
    monkeypatch.setattr(live_runner, "run_once", fake_run_once)
    monkeypatch.setattr(live_runner, "load_live_state", fake_load_live_state)
    monkeypatch.setattr(live_runner, "save_live_state", lambda path, state: None)
    monkeypatch.setattr(live_runner, "BinanceUMClient", FakeMarketClient)

    result = live_runner.run_live_loop(
        LiveRunConfig.from_argv(["--strategy", "rp_daily_breakout", "--dry-run", "--max-cycles", "1"]),
        sleep_fn=lambda _seconds: None,
        now_ms_fn=lambda: 3_100,
    )

    assert result == 0
    assert "submit_error_spike" not in capsys.readouterr().out


def test_run_live_loop_emits_submit_error_alert_from_submit_counters(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    class FakeStrategy:
        strategy_id = "rp_daily_breakout"

        def prepare_features(self, bars: pl.DataFrame, bars_htf: pl.DataFrame | None = None) -> pl.DataFrame:
            return bars

        def on_start(self, context) -> None:
            return None

        def on_bar(self, context) -> list[StrategyDecision]:
            return []

        def on_finish(self, context) -> None:
            return None

    class FakeMarketClient:
        pass

    def fake_build_strategy(strategy_id: str) -> FakeStrategy:
        assert strategy_id == "rp_daily_breakout"
        return FakeStrategy()

    def fake_fetch_closed_bars(*, client, symbol: str, interval: str, lookback_bars: int, now_ms: int) -> pl.DataFrame:
        assert isinstance(client, FakeMarketClient)
        return pl.DataFrame(
            {
                "open_time": [1_000, 2_000],
                "close_time": [1_999, 2_999],
                "close": [10.0, 11.0],
            }
        )

    def fake_run_once(**kwargs) -> dict[str, object]:
        return {
            "decisions_count": 0,
            "latest_open_time": 2_000,
            "risk_rejections": [],
            "submit_attempts_count": 100,
            "submit_errors_count": 5,
            "submit_count": 0,
        }

    def fake_load_live_state(path) -> LiveRuntimeState:
        return LiveRuntimeState(last_processed_open_time=1_000)

    monkeypatch.setattr(live_runner, "build_strategy", fake_build_strategy)
    monkeypatch.setattr(live_runner, "fetch_closed_bars", fake_fetch_closed_bars)
    monkeypatch.setattr(live_runner, "run_once", fake_run_once)
    monkeypatch.setattr(live_runner, "load_live_state", fake_load_live_state)
    monkeypatch.setattr(live_runner, "save_live_state", lambda path, state: None)
    monkeypatch.setattr(live_runner, "BinanceUMClient", FakeMarketClient)

    result = live_runner.run_live_loop(
        LiveRunConfig.from_argv(["--strategy", "rp_daily_breakout", "--dry-run", "--max-cycles", "1"]),
        sleep_fn=lambda _seconds: None,
        now_ms_fn=lambda: 3_100,
    )

    assert result == 0
    assert "live_runner alerts=submit_error_spike" in capsys.readouterr().out


def test_run_live_loop_accumulates_submit_counters_across_cycles(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cycle_bars = [
        pl.DataFrame(
            {
                "open_time": [1_000, 2_000],
                "close_time": [1_999, 2_999],
                "close": [10.0, 11.0],
            }
        ),
        pl.DataFrame(
            {
                "open_time": [1_000, 2_000],
                "close_time": [1_999, 2_999],
                "close": [10.0, 11.0],
            }
        ),
        pl.DataFrame(
            {
                "open_time": [1_000, 2_000, 3_000],
                "close_time": [1_999, 2_999, 3_999],
                "close": [10.0, 11.0, 12.0],
            }
        ),
    ]
    compute_calls: list[dict[str, int | float]] = []
    run_once_results = [
        {
            "decisions_count": 0,
            "latest_open_time": 2_000,
            "risk_rejections": [],
            "submit_attempts_count": 10,
            "submit_errors_count": 0,
            "submit_count": 0,
        },
        {
            "decisions_count": 0,
            "latest_open_time": 3_000,
            "risk_rejections": [],
            "submit_attempts_count": 5,
            "submit_errors_count": 1,
            "submit_count": 0,
        },
    ]

    class FakeStrategy:
        strategy_id = "rp_daily_breakout"

        def prepare_features(self, bars: pl.DataFrame, bars_htf: pl.DataFrame | None = None) -> pl.DataFrame:
            return bars

        def on_start(self, context) -> None:
            return None

        def on_bar(self, context) -> list[StrategyDecision]:
            return []

        def on_finish(self, context) -> None:
            return None

    class FakeMarketClient:
        pass

    def fake_build_strategy(strategy_id: str) -> FakeStrategy:
        assert strategy_id == "rp_daily_breakout"
        return FakeStrategy()

    def fake_fetch_closed_bars(*, client, symbol: str, interval: str, lookback_bars: int, now_ms: int) -> pl.DataFrame:
        assert isinstance(client, FakeMarketClient)
        return cycle_bars.pop(0)

    def fake_run_once(**kwargs) -> dict[str, object]:
        return run_once_results.pop(0)

    def fake_load_live_state(path) -> LiveRuntimeState:
        return LiveRuntimeState(last_processed_open_time=1_000)

    def fake_compute_runtime_alerts(
        *,
        peak_equity: float,
        current_equity: float,
        submit_attempts: int,
        submit_errors: int,
        ws_disconnects: int,
    ) -> list[str]:
        compute_calls.append(
            {
                "submit_attempts": submit_attempts,
                "submit_errors": submit_errors,
            }
        )
        if submit_attempts == 15 and submit_errors == 1:
            return ["submit_error_spike"]
        return []

    monkeypatch.setattr(live_runner, "build_strategy", fake_build_strategy)
    monkeypatch.setattr(live_runner, "fetch_closed_bars", fake_fetch_closed_bars)
    monkeypatch.setattr(live_runner, "run_once", fake_run_once)
    monkeypatch.setattr(live_runner, "load_live_state", fake_load_live_state)
    monkeypatch.setattr(live_runner, "save_live_state", lambda path, state: None)
    monkeypatch.setattr(live_runner, "BinanceUMClient", FakeMarketClient)
    monkeypatch.setattr(live_runner, "compute_runtime_alerts", fake_compute_runtime_alerts)

    result = live_runner.run_live_loop(
        LiveRunConfig.from_argv(["--strategy", "rp_daily_breakout", "--dry-run", "--max-cycles", "3"]),
        sleep_fn=lambda _seconds: None,
        now_ms_fn=lambda: 3_100,
    )

    assert result == 0
    assert compute_calls == [
        {"submit_attempts": 10, "submit_errors": 0},
        {"submit_attempts": 15, "submit_errors": 1},
    ]
    assert capsys.readouterr().out.count("live_runner alerts=submit_error_spike") == 1


@pytest.mark.parametrize("max_cycles", [0, -1])
def test_run_live_loop_skips_execution_when_max_cycles_non_positive(
    monkeypatch: pytest.MonkeyPatch,
    max_cycles: int,
) -> None:
    def fail_build_strategy(strategy_id: str) -> object:
        raise AssertionError("build_strategy should not be called when max_cycles is non-positive")

    def fail_bootstrap_live_runner(*, api_key: str, api_secret: str) -> object:
        raise AssertionError("bootstrap_live_runner should not be called when max_cycles is non-positive")

    def fail_fetch_closed_bars(**kwargs) -> pl.DataFrame:
        raise AssertionError("fetch_closed_bars should not be called when max_cycles is non-positive")

    monkeypatch.setattr(live_runner, "build_strategy", fail_build_strategy)
    monkeypatch.setattr(live_runner, "bootstrap_live_runner", fail_bootstrap_live_runner)
    monkeypatch.setattr(live_runner, "fetch_closed_bars", fail_fetch_closed_bars)

    result = live_runner.run_live_loop(
        LiveRunConfig.from_argv(
            [
                "--strategy",
                "rp_daily_breakout",
                "--dry-run",
                "--max-cycles",
                str(max_cycles),
            ]
        ),
        sleep_fn=lambda _seconds: None,
        now_ms_fn=lambda: 3_100,
    )

    assert result == 0


def test_main_runs_live_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[LiveRunConfig] = []

    def fake_run_live_loop(config: LiveRunConfig) -> int:
        calls.append(config)
        return 0

    monkeypatch.setattr(live_runner, "run_live_loop", fake_run_live_loop)

    result = live_runner.main(["--strategy", "rp_daily_breakout", "--dry-run", "--max-cycles", "1"])

    assert result == 0
    assert len(calls) == 1
    assert calls[0].max_cycles == 1
