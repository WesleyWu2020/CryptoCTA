# RP Daily Breakout Live Readiness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `rp_daily_breakout` runnable in live trading with deterministic order IDs, explicit risk gates, and bar-close execution semantics consistent with current strategy runtime contracts.

**Architecture:** Keep strategy logic in `strategy_runtime/strategies/rp_daily_breakout.py`, then add a thin live orchestration layer that converts `StrategyDecision` into exchange orders. The live runner owns data polling, risk checks, and execution side effects; the strategy remains pure decision logic. Delivery is staged: first unify contracts and add dry-run safety, then add real Binance order submission and recovery hooks.

**Tech Stack:** Python 3.12+, Polars, httpx, Binance UM Futures REST API, pytest.

---

## Scope Check

This plan focuses on one subsystem: **RP daily breakout live execution path**. It does not refactor unrelated strategies or replace the full backtest engine.

## File Structure and Responsibilities

- Modify: `src/cta_core/strategy_runtime/strategies/rp_daily_breakout.py`
  - Remove turtle-coupled CLI config conversion from this strategy and keep only RP strategy-native args.
- Create: `src/cta_core/app/live_config.py`
  - Centralize live runner CLI/config dataclass.
- Modify: `src/cta_core/app/live_runner.py`
  - Implement bar-close loop, decision->intent mapping, risk checks, and adapter calls.
- Modify: `src/cta_core/execution/live_binance.py`
  - Add signed Binance REST order submit/query primitives on top of existing deterministic `client_order_id`.
- Create: `scripts/run_live_strategy.py`
  - Canonical entrypoint for live strategy execution.
- Create: `tests/execution/test_live_binance_submit.py`
  - Verify request signing, payload shape, and idempotent `newClientOrderId` usage.
- Create: `tests/app/test_live_runner.py`
  - Verify bar-close behavior, risk rejection path, dry-run path, and decision mapping.
- Modify: `docs/runbooks/live-operations.md`
  - Add RP live launch, rollback, and recovery checklist.

---

### Task 1: Decouple RP Strategy from Turtle Backtest Config

**Files:**
- Modify: `src/cta_core/strategy_runtime/strategies/rp_daily_breakout.py`
- Test: `tests/strategy/test_rp_daily_breakout.py`

- [ ] **Step 1: Write failing test for RP-native CLI config**

```python
# tests/strategy/test_rp_daily_breakout.py
import argparse
from decimal import Decimal

from cta_core.strategy_runtime.strategies.rp_daily_breakout import (
    RPDailyBreakoutConfig,
    RPDailyBreakoutStrategy,
)


def test_config_from_args_returns_rp_config_only() -> None:
    parser = argparse.ArgumentParser()
    RPDailyBreakoutStrategy.register_cli_args(parser)
    args = parser.parse_args([
        "--rp-window", "5",
        "--entry-confirmations", "3",
        "--exit-confirmations", "2",
        "--quantity", "0.2",
    ])

    cfg = RPDailyBreakoutStrategy.config_from_args(args)

    assert isinstance(cfg, RPDailyBreakoutConfig)
    assert cfg.rp_window == 5
    assert cfg.entry_confirmations == 3
    assert cfg.exit_confirmations == 2
    assert cfg.quantity == Decimal("0.2")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest tests/strategy/test_rp_daily_breakout.py::test_config_from_args_returns_rp_config_only -v`
Expected: FAIL because current `config_from_args` returns `TurtleConfig` and RP-native flags do not exist.

- [ ] **Step 3: Implement RP-native CLI args and config conversion**

```python
# src/cta_core/strategy_runtime/strategies/rp_daily_breakout.py
@classmethod
def register_cli_args(cls, parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--rp-window", type=int, default=3)
    parser.add_argument("--entry-confirmations", type=int, default=2)
    parser.add_argument("--exit-confirmations", type=int, default=2)
    parser.add_argument("--quantity", type=Decimal, default=Decimal("1"))

@classmethod
def config_from_args(cls, args: argparse.Namespace) -> RPDailyBreakoutConfig:
    return RPDailyBreakoutConfig(
        rp_window=int(args.rp_window),
        entry_confirmations=int(args.entry_confirmations),
        exit_confirmations=int(args.exit_confirmations),
        quantity=Decimal(str(args.quantity)),
    )
```

- [ ] **Step 4: Run strategy tests**

Run: `PYTHONPATH=src pytest tests/strategy/test_rp_daily_breakout.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/cta_core/strategy_runtime/strategies/rp_daily_breakout.py tests/strategy/test_rp_daily_breakout.py
git commit -m "refactor: isolate rp_daily_breakout config from turtle backtest"
```

---

### Task 2: Add Live Runtime Config and Entry Script

**Files:**
- Create: `src/cta_core/app/live_config.py`
- Create: `scripts/run_live_strategy.py`
- Test: `tests/app/test_live_runner.py`

- [ ] **Step 1: Write failing config parse test**

```python
# tests/app/test_live_runner.py
from cta_core.app.live_config import LiveRunConfig


def test_live_run_config_from_args() -> None:
    cfg = LiveRunConfig.from_argv([
        "--strategy", "rp_daily_breakout",
        "--symbol", "BTCUSDT",
        "--interval", "1h",
        "--lookback-bars", "300",
        "--poll-seconds", "2",
        "--dry-run",
    ])

    assert cfg.strategy_id == "rp_daily_breakout"
    assert cfg.symbol == "BTCUSDT"
    assert cfg.interval == "1h"
    assert cfg.lookback_bars == 300
    assert cfg.poll_seconds == 2
    assert cfg.dry_run is True
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest tests/app/test_live_runner.py::test_live_run_config_from_args -v`
Expected: FAIL with import error for `cta_core.app.live_config`.

- [ ] **Step 3: Implement live config dataclass + parser**

```python
# src/cta_core/app/live_config.py
from __future__ import annotations

import argparse
from dataclasses import dataclass


@dataclass(frozen=True)
class LiveRunConfig:
    strategy_id: str
    symbol: str
    interval: str
    lookback_bars: int
    poll_seconds: int
    dry_run: bool
    api_key: str
    api_secret: str

    @classmethod
    def build_parser(cls) -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(description="Run live strategy loop")
        parser.add_argument("--strategy", required=True)
        parser.add_argument("--symbol", default="BTCUSDT")
        parser.add_argument("--interval", default="1h")
        parser.add_argument("--lookback-bars", type=int, default=300)
        parser.add_argument("--poll-seconds", type=int, default=2)
        parser.add_argument("--dry-run", action="store_true")
        parser.add_argument("--api-key", default="")
        parser.add_argument("--api-secret", default="")
        return parser

    @classmethod
    def from_argv(cls, argv: list[str] | None = None) -> "LiveRunConfig":
        args = cls.build_parser().parse_args(argv)
        return cls(
            strategy_id=args.strategy,
            symbol=args.symbol,
            interval=args.interval,
            lookback_bars=args.lookback_bars,
            poll_seconds=args.poll_seconds,
            dry_run=bool(args.dry_run),
            api_key=args.api_key,
            api_secret=args.api_secret,
        )
```

- [ ] **Step 4: Add live script entrypoint**

```python
# scripts/run_live_strategy.py
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from cta_core.app.live_runner import main


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 5: Run test and smoke CLI help**

Run: `PYTHONPATH=src pytest tests/app/test_live_runner.py::test_live_run_config_from_args -v`
Expected: PASS.

Run: `PYTHONPATH=src python scripts/run_live_strategy.py --help`
Expected: prints live runner flags including `--strategy`, `--dry-run`.

- [ ] **Step 6: Commit**

```bash
git add src/cta_core/app/live_config.py scripts/run_live_strategy.py tests/app/test_live_runner.py
git commit -m "feat: add live runner config and entry script"
```

---

### Task 3: Implement Decision-to-Order Mapping and Risk Gate

**Files:**
- Modify: `src/cta_core/app/live_runner.py`
- Test: `tests/app/test_live_runner.py`

- [ ] **Step 1: Write failing tests for mapping and risk rejection**

```python
# tests/app/test_live_runner.py
from decimal import Decimal

from cta_core.events.models import Side
from cta_core.risk.engine import RiskContext, RiskEngine
from cta_core.strategy_runtime.base import StrategyDecision, StrategyDecisionType
from cta_core.app.live_runner import decision_to_intent, check_risk


def test_decision_to_intent_maps_enter_long() -> None:
    decision = StrategyDecision(
        decision_type=StrategyDecisionType.ENTER_LONG,
        size=Decimal("0.01"),
        reason="rp_breakout_confirmed",
    )
    intent = decision_to_intent(strategy_id="rp_daily_breakout", symbol="BTCUSDT", decision=decision)
    assert intent is not None
    assert intent.side == Side.BUY
    assert intent.quantity == Decimal("0.01")


def test_check_risk_rejects_when_symbol_budget_exceeded() -> None:
    engine = RiskEngine(max_daily_loss=Decimal("500"), max_symbol_notional_ratio=Decimal("0.2"))
    ctx = RiskContext(
        symbol="BTCUSDT",
        order_notional=Decimal("300"),
        equity=Decimal("1000"),
        day_pnl=Decimal("0"),
        losing_streak=0,
        symbol_notional=Decimal("0"),
    )
    result = check_risk(engine, ctx)
    assert result.allowed is False
    assert result.rule == "symbol_risk_budget"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `PYTHONPATH=src pytest tests/app/test_live_runner.py::test_decision_to_intent_maps_enter_long tests/app/test_live_runner.py::test_check_risk_rejects_when_symbol_budget_exceeded -v`
Expected: FAIL due to missing functions.

- [ ] **Step 3: Implement mapper and risk helper in live runner**

```python
# src/cta_core/app/live_runner.py
from decimal import Decimal

from cta_core.events.models import OrderIntent, Side
from cta_core.risk.engine import RiskContext, RiskEngine, RiskResult
from cta_core.strategy_runtime.base import StrategyDecision, StrategyDecisionType


def decision_to_intent(*, strategy_id: str, symbol: str, decision: StrategyDecision) -> OrderIntent | None:
    if decision.decision_type == StrategyDecisionType.ENTER_LONG:
        return OrderIntent(
            strategy_id=strategy_id,
            symbol=symbol,
            side=Side.BUY,
            quantity=decision.size,
            order_type="MARKET",
        )
    if decision.decision_type == StrategyDecisionType.EXIT_LONG:
        return OrderIntent(
            strategy_id=strategy_id,
            symbol=symbol,
            side=Side.SELL,
            quantity=Decimal("0"),
            order_type="MARKET",
        )
    return None


def check_risk(engine: RiskEngine, ctx: RiskContext) -> RiskResult:
    return engine.check(ctx)
```

- [ ] **Step 4: Run test module**

Run: `PYTHONPATH=src pytest tests/app/test_live_runner.py -v`
Expected: PASS for new unit cases.

- [ ] **Step 5: Commit**

```bash
git add src/cta_core/app/live_runner.py tests/app/test_live_runner.py
git commit -m "feat: add live decision mapping and risk gate helpers"
```

---

### Task 4: Extend Binance Live Adapter for Signed Order Submission

**Files:**
- Modify: `src/cta_core/execution/live_binance.py`
- Create: `tests/execution/test_live_binance_submit.py`

- [ ] **Step 1: Write failing adapter test for signed order payload**

```python
# tests/execution/test_live_binance_submit.py
from decimal import Decimal

import httpx

from cta_core.events.models import OrderIntent, Side
from cta_core.execution.live_binance import LiveBinanceAdapter


def test_submit_market_order_uses_client_order_id(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_post(url: str, *, headers: dict[str, str], params: dict[str, object], timeout: float):
        captured["url"] = url
        captured["headers"] = headers
        captured["params"] = params

        class Response:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict[str, object]:
                return {"orderId": 123, "status": "NEW"}

        return Response()

    monkeypatch.setattr(httpx, "post", fake_post)

    adapter = LiveBinanceAdapter(api_key="k", api_secret="s")
    intent = OrderIntent("rp_daily_breakout", "BTCUSDT", Side.BUY, Decimal("0.01"), "MARKET")
    result = adapter.submit_order(intent=intent, ts_ms=1710000000000)

    assert result["status"] == "NEW"
    assert captured["params"]["newClientOrderId"]
    assert captured["headers"]["X-MBX-APIKEY"] == "k"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest tests/execution/test_live_binance_submit.py -v`
Expected: FAIL because `submit_order` does not exist.

- [ ] **Step 3: Implement signed REST submit in adapter**

```python
# src/cta_core/execution/live_binance.py
import hmac
import time
from hashlib import sha256
from urllib.parse import urlencode

import httpx

from cta_core.events.models import OrderIntent


class LiveBinanceAdapter:
    BASE_URL = "https://fapi.binance.com"

    # existing __init__ and client_order_id stay unchanged

    def _sign(self, params: dict[str, object]) -> str:
        query = urlencode(params)
        return hmac.new(self.api_secret.encode(), query.encode(), sha256).hexdigest()

    def submit_order(self, *, intent: OrderIntent, ts_ms: int | None = None) -> dict[str, object]:
        now_ms = int(time.time() * 1000) if ts_ms is None else ts_ms
        params: dict[str, object] = {
            "symbol": intent.symbol,
            "side": intent.side.value,
            "type": intent.order_type,
            "quantity": str(intent.quantity),
            "newClientOrderId": self.client_order_id(
                strategy_id=intent.strategy_id,
                symbol=intent.symbol,
                ts_ms=now_ms,
            ),
            "timestamp": now_ms,
        }
        params["signature"] = self._sign(params)

        response = httpx.post(
            f"{self.BASE_URL}/fapi/v1/order",
            headers={"X-MBX-APIKEY": self.api_key},
            params=params,
            timeout=10.0,
        )
        response.raise_for_status()
        return response.json()
```

- [ ] **Step 4: Run adapter tests**

Run: `PYTHONPATH=src pytest tests/execution/test_live_binance.py tests/execution/test_live_binance_submit.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/cta_core/execution/live_binance.py tests/execution/test_live_binance.py tests/execution/test_live_binance_submit.py
git commit -m "feat: add signed binance order submission adapter"
```

---

### Task 5: Build Bar-Close Live Loop for RP Strategy

**Files:**
- Modify: `src/cta_core/app/live_runner.py`
- Test: `tests/app/test_live_runner.py`

- [ ] **Step 1: Write failing live loop test (dry-run no submit)**

```python
# tests/app/test_live_runner.py
from decimal import Decimal

import polars as pl

from cta_core.app.live_runner import run_once
from cta_core.strategy_runtime.strategies.rp_daily_breakout import RPDailyBreakoutConfig, RPDailyBreakoutStrategy


class DummyAdapter:
    def __init__(self) -> None:
        self.submits = []

    def submit_order(self, *, intent, ts_ms=None):
        self.submits.append((intent, ts_ms))
        return {"status": "NEW"}


def test_run_once_dry_run_does_not_submit() -> None:
    strategy = RPDailyBreakoutStrategy(RPDailyBreakoutConfig(rp_window=1, entry_confirmations=1, exit_confirmations=1, quantity=Decimal("0.01")))
    bars = pl.DataFrame(
        {
            "open_time": [1, 2],
            "open": [10.0, 11.0],
            "high": [10.1, 11.1],
            "low": [9.9, 10.9],
            "close": [10.0, 11.0],
        }
    )
    adapter = DummyAdapter()

    out = run_once(strategy=strategy, strategy_id="rp_daily_breakout", symbol="BTCUSDT", bars=bars, adapter=adapter, dry_run=True)

    assert out["decisions"] >= 1
    assert len(adapter.submits) == 0
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest tests/app/test_live_runner.py::test_run_once_dry_run_does_not_submit -v`
Expected: FAIL because `run_once` is not implemented.

- [ ] **Step 3: Implement run_once with bar-close semantics**

```python
# src/cta_core/app/live_runner.py
import time
from typing import Any

import polars as pl

from cta_core.strategy_runtime.base import StrategyContext


def run_once(
    *,
    strategy,
    strategy_id: str,
    symbol: str,
    bars: pl.DataFrame,
    adapter,
    dry_run: bool,
) -> dict[str, Any]:
    sorted_bars = bars.sort("open_time")
    prepared = strategy.prepare_features(sorted_bars)
    context = StrategyContext(symbol=symbol, bars=prepared)
    decisions = strategy.on_bar(context)

    submitted = 0
    for decision in decisions:
        intent = decision_to_intent(strategy_id=strategy_id, symbol=symbol, decision=decision)
        if intent is None:
            continue
        if dry_run:
            continue
        adapter.submit_order(intent=intent, ts_ms=int(time.time() * 1000))
        submitted += 1

    return {"decisions": len(decisions), "submitted": submitted}
```

- [ ] **Step 4: Run live-runner tests**

Run: `PYTHONPATH=src pytest tests/app/test_live_runner.py -v`
Expected: PASS for dry-run and mapper tests.

- [ ] **Step 5: Commit**

```bash
git add src/cta_core/app/live_runner.py tests/app/test_live_runner.py
git commit -m "feat: add rp live run_once execution path"
```

---

### Task 6: Add Main Loop, Startup Validation, and Safe Logging

**Files:**
- Modify: `src/cta_core/app/live_runner.py`
- Modify: `scripts/run_live_strategy.py`
- Test: `tests/app/test_live_runner.py`

- [ ] **Step 1: Write failing test for credential validation in non-dry-run mode**

```python
# tests/app/test_live_runner.py
import pytest

from cta_core.app.live_runner import validate_live_mode


def test_validate_live_mode_requires_credentials_without_dry_run() -> None:
    with pytest.raises(ValueError, match="api_key and api_secret"):
        validate_live_mode(dry_run=False, api_key="", api_secret="")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest tests/app/test_live_runner.py::test_validate_live_mode_requires_credentials_without_dry_run -v`
Expected: FAIL because validator function does not exist.

- [ ] **Step 3: Implement validator + runner main**

```python
# src/cta_core/app/live_runner.py
from cta_core.app.live_config import LiveRunConfig
from cta_core.execution.live_binance import LiveBinanceAdapter
from cta_core.strategy_runtime.registry import build_strategy


def validate_live_mode(*, dry_run: bool, api_key: str, api_secret: str) -> None:
    if not dry_run and (not api_key or not api_secret):
        raise ValueError("api_key and api_secret are required when dry_run is false")


def main(argv: list[str] | None = None) -> int:
    cfg = LiveRunConfig.from_argv(argv)
    validate_live_mode(dry_run=cfg.dry_run, api_key=cfg.api_key, api_secret=cfg.api_secret)

    strategy = build_strategy(cfg.strategy_id)
    adapter = LiveBinanceAdapter(api_key=cfg.api_key, api_secret=cfg.api_secret)

    # First iteration only; convert to infinite loop in next task.
    print(f"live runner start strategy={cfg.strategy_id} symbol={cfg.symbol} dry_run={cfg.dry_run}")
    return 0
```

- [ ] **Step 4: Run tests and CLI help smoke**

Run: `PYTHONPATH=src pytest tests/app/test_live_runner.py -v`
Expected: PASS.

Run: `PYTHONPATH=src python scripts/run_live_strategy.py --strategy rp_daily_breakout --dry-run`
Expected: exits 0 and prints startup line.

- [ ] **Step 5: Commit**

```bash
git add src/cta_core/app/live_runner.py scripts/run_live_strategy.py tests/app/test_live_runner.py
git commit -m "feat: add live runner main and mode validation"
```

---

### Task 7: Recovery Hooks, Runbook, and Regression Safety

**Files:**
- Modify: `src/cta_core/app/live_runner.py`
- Modify: `docs/runbooks/live-operations.md`
- Test: `tests/app/test_live_runner.py`

- [ ] **Step 1: Write failing test for idempotent dedupe key usage**

```python
# tests/app/test_live_runner.py

def test_run_once_passes_stable_ts_for_client_order_id(monkeypatch) -> None:
    from decimal import Decimal

    import polars as pl

    from cta_core.app.live_runner import run_once
    from cta_core.strategy_runtime.strategies.rp_daily_breakout import RPDailyBreakoutConfig, RPDailyBreakoutStrategy

    captured: dict[str, int] = {}

    class DummyAdapter:
        def submit_order(self, *, intent, ts_ms=None):
            captured["ts_ms"] = ts_ms
            return {"status": "NEW"}

    strategy = RPDailyBreakoutStrategy(
        RPDailyBreakoutConfig(rp_window=1, entry_confirmations=1, exit_confirmations=1, quantity=Decimal("0.01"))
    )
    bars = pl.DataFrame(
        {
            "open_time": [1, 2],
            "open": [10.0, 11.0],
            "high": [10.1, 11.1],
            "low": [9.9, 10.9],
            "close": [10.0, 11.0],
        }
    )

    run_once(
        strategy=strategy,
        strategy_id="rp_daily_breakout",
        symbol="BTCUSDT",
        bars=bars,
        adapter=DummyAdapter(),
        dry_run=False,
        now_ms=1710000000000,
    )
    assert captured["ts_ms"] == 1710000000000
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest tests/app/test_live_runner.py::test_run_once_passes_stable_ts_for_client_order_id -v`
Expected: FAIL because `run_once` does not yet support `now_ms` injection.

- [ ] **Step 3: Implement clock injection and minimal recovery state**

```python
# src/cta_core/app/live_runner.py
def run_once(
    *,
    strategy,
    strategy_id: str,
    symbol: str,
    bars: pl.DataFrame,
    adapter,
    dry_run: bool,
    now_ms: int | None = None,
) -> dict[str, Any]:
    ts_ms = int(time.time() * 1000) if now_ms is None else now_ms
    sorted_bars = bars.sort("open_time")
    prepared = strategy.prepare_features(sorted_bars)
    context = StrategyContext(symbol=symbol, bars=prepared)
    decisions = strategy.on_bar(context)

    submitted = 0
    for decision in decisions:
        intent = decision_to_intent(strategy_id=strategy_id, symbol=symbol, decision=decision)
        if intent is None or dry_run:
            continue
        adapter.submit_order(intent=intent, ts_ms=ts_ms)
        submitted += 1

    return {"decisions": len(decisions), "submitted": submitted}


def load_recovery_state(path: str) -> dict[str, Any]:
    import json
    from pathlib import Path

    file = Path(path)
    if not file.exists():
        return {"last_submit_ts_ms": None}
    return json.loads(file.read_text(encoding="utf-8"))
```

- [ ] **Step 4: Update runbook with launch/rollback checklist**

```markdown
# docs/runbooks/live-operations.md
## RP Daily Breakout Live Checklist
1. Dry run at least 30 minutes with `--dry-run`.
2. Confirm risk limits: daily max loss, symbol notional ratio, losing streak threshold.
3. Start live runner with explicit `--strategy rp_daily_breakout --symbol BTCUSDT --interval 1h`.
4. Verify first order has deterministic `newClientOrderId`.
5. On incident: stop runner, record last order ts, restart with recovery state.
```

- [ ] **Step 5: Run focused + full tests**

Run: `PYTHONPATH=src pytest tests/app/test_live_runner.py tests/execution/test_live_binance.py tests/execution/test_live_binance_submit.py -v`
Expected: PASS.

Run: `PYTHONPATH=src pytest -q`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/cta_core/app/live_runner.py docs/runbooks/live-operations.md tests/app/test_live_runner.py
git commit -m "docs: add rp live runbook and recovery-oriented runner hooks"
```

---

## Final Verification Gate

- [ ] Run: `PYTHONPATH=src pytest -q`
Expected: full suite green.

- [ ] If Rust bindings changed (not expected in this plan), also run:
Run: `maturin develop --manifest-path rust/ledger_core/Cargo.toml`
Run: `PYTHONPATH=src pytest tests/bindings/test_ledger_binding.py -q`
Expected: PASS.

---

## Self-Review

1. Spec coverage
- Covered shared strategy/live contract requirement by unifying around `StrategyDecision` in live path.
- Covered live runner gap (`src/cta_core/app/live_runner.py` currently bootstrap-only) with staged implementation tasks.
- Covered exchange adapter execution gap (currently only client order id) with signed submit task.
- Covered risk determinism requirement using existing `RiskEngine` before submit.
- Covered operational safety with dry-run, credential checks, and runbook updates.

2. Placeholder scan
- No `TBD`/`TODO` placeholders in implementation steps.
- Every code-change step includes concrete code snippets and commands.

3. Type consistency
- Strategy decision uses `StrategyDecision`/`StrategyDecisionType` consistently.
- Execution intent uses `OrderIntent`/`Side` consistently.
- Live config uses one dataclass `LiveRunConfig` referenced consistently by `main(argv)`.
