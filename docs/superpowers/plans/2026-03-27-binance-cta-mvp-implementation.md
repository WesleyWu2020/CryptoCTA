# Binance USDT-M CTA MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a local Python+Rust CTA system for Binance USDT-M perpetuals with anti-lookahead backtesting and safe live trading for a single strategy/single portfolio.

**Architecture:** Python owns orchestration (data ingestion, strategy runtime, risk checks, exchange adapter), while Rust owns fill accounting and portfolio ledger math. Backtest and live share one event contract and one ledger API to prevent semantic drift. Data is persisted in Parquet and queried through DuckDB, with runtime access guarded by a lookahead-safe DataPortal.

**Tech Stack:** Python 3.12, pytest, polars, duckdb, pyarrow, httpx, pydantic, Rust 1.78, pyo3, maturin, GitHub Actions.

---

## Scope Decomposition

This spec spans multiple subsystems, so execution is staged into four independently testable milestones in one coordinated plan:
- M1 Data foundation (ingestion + storage + guarded reads)
- M2 Backtest core (strategy runtime + risk + sim execution + replay)
- M3 Live bridge (Binance adapter + order lifecycle + restart recovery)
- M4 Hardening (monitoring, alerts, CI, runbook)

## File Structure (Target)

- `pyproject.toml`: Python package metadata and dependencies
- `src/cta_core/config/settings.py`: runtime config model
- `src/cta_core/events/models.py`: canonical event and order/position contracts
- `src/cta_core/data/binance_client.py`: Binance UM futures API client
- `src/cta_core/data/ingest.py`: normalize/validate kline payloads
- `src/cta_core/data/parquet_store.py`: append/dedupe Parquet writes + DuckDB registration
- `src/cta_core/data/data_portal.py`: anti-lookahead read API for runtime
- `src/cta_core/strategy_runtime/interfaces.py`: strategy interface + context types
- `src/cta_core/strategy_runtime/runtime.py`: bar-close orchestration
- `src/cta_core/strategy_runtime/strategies/sma_cross.py`: sample CTA strategy
- `src/cta_core/risk/engine.py`: leverage/risk-budget/daily-loss/losing-streak checks
- `src/cta_core/execution/sim_engine.py`: simulated execution with slippage/fees/funding
- `src/cta_core/execution/live_binance.py`: live order submit/query/cancel adapter
- `src/cta_core/app/backtest_runner.py`: deterministic backtest orchestrator
- `src/cta_core/app/live_runner.py`: live loop with recovery/bootstrap
- `src/cta_core/ops/monitoring.py`: metrics and alert trigger logic
- `src/cta_core/bindings/ledger.py`: Python wrapper around Rust ledger extension
- `rust/ledger_core/Cargo.toml`: Rust crate config
- `rust/ledger_core/src/lib.rs`: ledger state transitions and PnL math
- `tests/...`: unit/integration tests by domain
- `.github/workflows/ci.yml`: lint/test/build pipeline
- `docs/runbooks/live-operations.md`: production operation and incident workflow

### Task 1: Bootstrap Repo Skeleton and Configuration

**Files:**
- Create: `pyproject.toml`
- Create: `src/cta_core/__init__.py`
- Create: `src/cta_core/config/settings.py`
- Create: `tests/config/test_settings.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/config/test_settings.py
from cta_core.config.settings import AppSettings


def test_settings_defaults():
    settings = AppSettings(
        symbols=["BTCUSDT", "ETHUSDT"],
        intervals=["15m", "1h"],
    )
    assert settings.timezone == "UTC"
    assert settings.exchange == "binance_um"
    assert settings.symbols == ["BTCUSDT", "ETHUSDT"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/config/test_settings.py -v`  
Expected: FAIL with `ModuleNotFoundError: No module named 'cta_core'`

- [ ] **Step 3: Write minimal implementation**

```toml
# pyproject.toml
[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "cryptocta"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
  "pydantic>=2.8",
  "duckdb>=1.0.0",
  "polars>=1.10.0",
  "pyarrow>=17.0.0",
  "httpx>=0.27.0",
]

[project.optional-dependencies]
dev = ["pytest>=8.3.0", "pytest-asyncio>=0.24.0", "maturin>=1.7.0", "ruff>=0.6.0"]

[tool.setuptools.packages.find]
where = ["src"]
```

```python
# src/cta_core/__init__.py
__version__ = "0.1.0"
```

```python
# src/cta_core/config/settings.py
from pydantic import BaseModel, Field


class AppSettings(BaseModel):
    exchange: str = "binance_um"
    timezone: str = "UTC"
    symbols: list[str] = Field(min_length=1)
    intervals: list[str] = Field(min_length=1)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src pytest tests/config/test_settings.py -v`  
Expected: PASS with `1 passed`

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml src/cta_core/__init__.py src/cta_core/config/settings.py tests/config/test_settings.py
git commit -m "chore: bootstrap python package and settings model"
```

### Task 2: Define Canonical Event and Order Contracts

**Files:**
- Create: `src/cta_core/events/models.py`
- Create: `tests/events/test_event_models.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/events/test_event_models.py
from decimal import Decimal

from cta_core.events.models import EventType, FillEvent, OrderIntent, Side


def test_fill_event_contract():
    fill = FillEvent(
        event_id="e1",
        symbol="BTCUSDT",
        side=Side.BUY,
        quantity=Decimal("0.01"),
        price=Decimal("62000"),
        fee=Decimal("0.5"),
        ts_ms=1700000000000,
    )
    assert fill.type == EventType.FILL_RECEIVED
    assert fill.notional == Decimal("620")


def test_order_intent_contract():
    intent = OrderIntent(
        strategy_id="sma_cross",
        symbol="BTCUSDT",
        side=Side.SELL,
        quantity=Decimal("0.02"),
        order_type="MARKET",
    )
    assert intent.order_type == "MARKET"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest tests/events/test_event_models.py -v`  
Expected: FAIL with import error for `cta_core.events.models`

- [ ] **Step 3: Write minimal implementation**

```python
# src/cta_core/events/models.py
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum


class EventType(str, Enum):
    BAR_CLOSED = "BAR_CLOSED"
    SIGNAL_GENERATED = "SIGNAL_GENERATED"
    RISK_CHECKED = "RISK_CHECKED"
    ORDER_SUBMITTED = "ORDER_SUBMITTED"
    FILL_RECEIVED = "FILL_RECEIVED"
    PORTFOLIO_UPDATED = "PORTFOLIO_UPDATED"


class Side(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass(frozen=True)
class OrderIntent:
    strategy_id: str
    symbol: str
    side: Side
    quantity: Decimal
    order_type: str
    limit_price: Decimal | None = None


@dataclass(frozen=True)
class FillEvent:
    event_id: str
    symbol: str
    side: Side
    quantity: Decimal
    price: Decimal
    fee: Decimal
    ts_ms: int
    type: EventType = EventType.FILL_RECEIVED

    @property
    def notional(self) -> Decimal:
        return self.quantity * self.price
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src pytest tests/events/test_event_models.py -v`  
Expected: PASS with `2 passed`

- [ ] **Step 5: Commit**

```bash
git add src/cta_core/events/models.py tests/events/test_event_models.py
git commit -m "feat: add canonical event and order contracts"
```

### Task 3: Build Kline Ingestion and Parquet Persistence (M1)

**Files:**
- Create: `src/cta_core/data/binance_client.py`
- Create: `src/cta_core/data/ingest.py`
- Create: `src/cta_core/data/parquet_store.py`
- Create: `tests/data/test_ingest_and_store.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/data/test_ingest_and_store.py
from pathlib import Path

import polars as pl

from cta_core.data.ingest import normalize_klines
from cta_core.data.parquet_store import append_closed_bars


def test_normalize_and_append(tmp_path: Path):
    raw = [[1700000000000, "62000", "62500", "61800", "62400", "123.4", 1700000899999, "0", 0, "0", "0", "0"]]
    df = normalize_klines(symbol="BTCUSDT", interval="15m", rows=raw)
    assert df.select("symbol").item() == "BTCUSDT"
    path = tmp_path / "bars.parquet"
    append_closed_bars(df, path)
    loaded = pl.read_parquet(path)
    assert loaded.height == 1
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest tests/data/test_ingest_and_store.py -v`  
Expected: FAIL with import error for data modules

- [ ] **Step 3: Write minimal implementation**

```python
# src/cta_core/data/ingest.py
import polars as pl


def normalize_klines(symbol: str, interval: str, rows: list[list]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": [symbol for _ in rows],
            "interval": [interval for _ in rows],
            "open_time": [int(r[0]) for r in rows],
            "open": [float(r[1]) for r in rows],
            "high": [float(r[2]) for r in rows],
            "low": [float(r[3]) for r in rows],
            "close": [float(r[4]) for r in rows],
            "volume": [float(r[5]) for r in rows],
            "close_time": [int(r[6]) for r in rows],
        }
    )
```

```python
# src/cta_core/data/parquet_store.py
from pathlib import Path

import polars as pl


def append_closed_bars(df: pl.DataFrame, path: Path) -> None:
    if path.exists():
        existing = pl.read_parquet(path)
        merged = pl.concat([existing, df]).unique(subset=["symbol", "interval", "open_time"])
        merged.write_parquet(path)
        return
    df.write_parquet(path)
```

```python
# src/cta_core/data/binance_client.py
from typing import Any

import httpx


class BinanceUMClient:
    BASE_URL = "https://fapi.binance.com"

    def fetch_klines(self, symbol: str, interval: str, limit: int = 1000) -> list[list[Any]]:
        response = httpx.get(
            f"{self.BASE_URL}/fapi/v1/klines",
            params={"symbol": symbol, "interval": interval, "limit": limit},
            timeout=10.0,
        )
        response.raise_for_status()
        return response.json()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src pytest tests/data/test_ingest_and_store.py -v`  
Expected: PASS with `1 passed`

- [ ] **Step 5: Commit**

```bash
git add src/cta_core/data/binance_client.py src/cta_core/data/ingest.py src/cta_core/data/parquet_store.py tests/data/test_ingest_and_store.py
git commit -m "feat: add kline normalization and parquet persistence"
```

### Task 4: Add Lookahead-Safe DataPortal (M1)

**Files:**
- Create: `src/cta_core/data/data_portal.py`
- Create: `tests/data/test_data_portal.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/data/test_data_portal.py
import polars as pl
import pytest

from cta_core.data.data_portal import DataPortal, FutureDataAccessError


def test_data_portal_rejects_future_access():
    df = pl.DataFrame(
        {
            "symbol": ["BTCUSDT", "BTCUSDT"],
            "interval": ["15m", "15m"],
            "open_time": [1000, 2000],
            "close": [10.0, 11.0],
        }
    )
    portal = DataPortal(df, latest_open_time=2000)
    with pytest.raises(FutureDataAccessError):
        portal.closed_bars("BTCUSDT", "15m", end_open_time=3000, lookback=2)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest tests/data/test_data_portal.py -v`  
Expected: FAIL with import error for `DataPortal`

- [ ] **Step 3: Write minimal implementation**

```python
# src/cta_core/data/data_portal.py
import polars as pl


class FutureDataAccessError(ValueError):
    pass


class DataPortal:
    def __init__(self, bars: pl.DataFrame, latest_open_time: int):
        self._bars = bars
        self._latest_open_time = latest_open_time

    def closed_bars(self, symbol: str, interval: str, end_open_time: int, lookback: int) -> pl.DataFrame:
        if end_open_time > self._latest_open_time:
            raise FutureDataAccessError(f"requested={end_open_time}, latest={self._latest_open_time}")
        return (
            self._bars
            .filter((pl.col("symbol") == symbol) & (pl.col("interval") == interval) & (pl.col("open_time") <= end_open_time))
            .sort("open_time")
            .tail(lookback)
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src pytest tests/data/test_data_portal.py -v`  
Expected: PASS with `1 passed`

- [ ] **Step 5: Commit**

```bash
git add src/cta_core/data/data_portal.py tests/data/test_data_portal.py
git commit -m "feat: enforce anti-lookahead data portal"
```

### Task 5: Implement Strategy Runtime and Sample Strategy (M2)

**Files:**
- Create: `src/cta_core/strategy_runtime/interfaces.py`
- Create: `src/cta_core/strategy_runtime/runtime.py`
- Create: `src/cta_core/strategy_runtime/strategies/sma_cross.py`
- Create: `tests/strategy/test_runtime.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/strategy/test_runtime.py
import polars as pl

from cta_core.strategy_runtime.runtime import run_bar_close
from cta_core.strategy_runtime.strategies.sma_cross import SmaCrossStrategy


def test_sma_cross_emits_order_intent():
    bars = pl.DataFrame({"close": [10.0, 10.5, 11.0, 12.0, 13.0]})
    strategy = SmaCrossStrategy(fast=2, slow=3)
    intent = run_bar_close(strategy=strategy, bars=bars, symbol="BTCUSDT")
    assert intent is not None
    assert intent.symbol == "BTCUSDT"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest tests/strategy/test_runtime.py -v`  
Expected: FAIL with import error for runtime modules

- [ ] **Step 3: Write minimal implementation**

```python
# src/cta_core/strategy_runtime/interfaces.py
from dataclasses import dataclass

import polars as pl

from cta_core.events.models import OrderIntent


@dataclass(frozen=True)
class StrategyContext:
    symbol: str
    bars: pl.DataFrame


class Strategy:
    strategy_id: str

    def on_bar_close(self, ctx: StrategyContext) -> OrderIntent | None:
        raise NotImplementedError
```

```python
# src/cta_core/strategy_runtime/strategies/sma_cross.py
from decimal import Decimal

import polars as pl

from cta_core.events.models import OrderIntent, Side
from cta_core.strategy_runtime.interfaces import Strategy, StrategyContext


class SmaCrossStrategy(Strategy):
    strategy_id = "sma_cross"

    def __init__(self, fast: int, slow: int):
        self.fast = fast
        self.slow = slow

    def on_bar_close(self, ctx: StrategyContext) -> OrderIntent | None:
        close = ctx.bars["close"]
        if len(close) < self.slow + 1:
            return None
        fast_ma = close.tail(self.fast).mean()
        slow_ma = close.tail(self.slow).mean()
        if fast_ma > slow_ma:
            return OrderIntent(self.strategy_id, ctx.symbol, Side.BUY, Decimal("0.01"), "MARKET")
        return None
```

```python
# src/cta_core/strategy_runtime/runtime.py
import polars as pl

from cta_core.strategy_runtime.interfaces import Strategy, StrategyContext


def run_bar_close(strategy: Strategy, bars: pl.DataFrame, symbol: str):
    return strategy.on_bar_close(StrategyContext(symbol=symbol, bars=bars))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src pytest tests/strategy/test_runtime.py -v`  
Expected: PASS with `1 passed`

- [ ] **Step 5: Commit**

```bash
git add src/cta_core/strategy_runtime/interfaces.py src/cta_core/strategy_runtime/runtime.py src/cta_core/strategy_runtime/strategies/sma_cross.py tests/strategy/test_runtime.py
git commit -m "feat: add bar-close strategy runtime and sample sma strategy"
```

### Task 6: Build Rust Ledger Core and Python Binding (M2)

**Files:**
- Create: `rust/ledger_core/Cargo.toml`
- Create: `rust/ledger_core/src/lib.rs`
- Create: `src/cta_core/bindings/ledger.py`
- Create: `tests/bindings/test_ledger_binding.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/bindings/test_ledger_binding.py
from decimal import Decimal

from cta_core.bindings.ledger import apply_fill
from cta_core.events.models import Side


def test_apply_fill_updates_position():
    state = {"position_qty": Decimal("0"), "avg_price": Decimal("0"), "realized_pnl": Decimal("0")}
    next_state = apply_fill(state, side=Side.BUY, qty=Decimal("1"), price=Decimal("100"))
    assert next_state["position_qty"] == Decimal("1")
    assert next_state["avg_price"] == Decimal("100")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest tests/bindings/test_ledger_binding.py -v`  
Expected: FAIL with import error for `cta_core.bindings.ledger`

- [ ] **Step 3: Write minimal implementation**

```toml
# rust/ledger_core/Cargo.toml
[package]
name = "ledger_core"
version = "0.1.0"
edition = "2021"

[lib]
name = "cta_ledger"
crate-type = ["cdylib"]

[dependencies]
pyo3 = { version = "0.22.0", features = ["extension-module"] }
```

```rust
// rust/ledger_core/src/lib.rs
use pyo3::prelude::*;

#[pyfunction]
fn apply_fill_py(position_qty: f64, avg_price: f64, side: &str, qty: f64, price: f64) -> (f64, f64) {
    if side == "BUY" {
        let next_qty = position_qty + qty;
        let next_avg = if next_qty == 0.0 {
            0.0
        } else {
            ((position_qty * avg_price) + (qty * price)) / next_qty
        };
        (next_qty, next_avg)
    } else {
        (position_qty - qty, avg_price)
    }
}

#[pymodule]
fn cta_ledger(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(apply_fill_py, m)?)?;
    Ok(())
}
```

```python
# src/cta_core/bindings/ledger.py
from decimal import Decimal

import cta_ledger

from cta_core.events.models import Side


def apply_fill(state: dict, side: Side, qty: Decimal, price: Decimal) -> dict:
    next_qty, next_avg = cta_ledger.apply_fill_py(
        float(state["position_qty"]),
        float(state["avg_price"]),
        side.value,
        float(qty),
        float(price),
    )
    return {
        "position_qty": Decimal(str(next_qty)),
        "avg_price": Decimal(str(next_avg)),
        "realized_pnl": state["realized_pnl"],
    }
```

- [ ] **Step 4: Build extension and run test to verify it passes**

Run: `maturin develop --manifest-path rust/ledger_core/Cargo.toml && PYTHONPATH=src pytest tests/bindings/test_ledger_binding.py -v`  
Expected: PASS with `1 passed`

- [ ] **Step 5: Commit**

```bash
git add rust/ledger_core/Cargo.toml rust/ledger_core/src/lib.rs src/cta_core/bindings/ledger.py tests/bindings/test_ledger_binding.py
git commit -m "feat: add rust ledger extension and python binding"
```

### Task 7: Implement Sim Execution with Slippage/Fees/Funding (M2)

**Files:**
- Create: `src/cta_core/execution/sim_engine.py`
- Create: `tests/execution/test_sim_engine.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/execution/test_sim_engine.py
from decimal import Decimal

from cta_core.events.models import OrderIntent, Side
from cta_core.execution.sim_engine import simulate_fill


def test_simulate_fill_applies_directional_slippage_and_fee():
    intent = OrderIntent("sma_cross", "BTCUSDT", Side.BUY, Decimal("0.01"), "MARKET")
    fill = simulate_fill(
        intent=intent,
        next_open=Decimal("60000"),
        bar_high=Decimal("60600"),
        bar_low=Decimal("59400"),
        fee_bps=Decimal("4"),
        base_slippage_bps=Decimal("1"),
        k=Decimal("0.2"),
    )
    assert fill.price > Decimal("60000")
    assert fill.fee > Decimal("0")
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest tests/execution/test_sim_engine.py -v`  
Expected: FAIL with import error for `sim_engine`

- [ ] **Step 3: Write minimal implementation**

```python
# src/cta_core/execution/sim_engine.py
from decimal import Decimal

from cta_core.events.models import FillEvent, OrderIntent, Side


def simulate_fill(
    intent: OrderIntent,
    next_open: Decimal,
    bar_high: Decimal,
    bar_low: Decimal,
    fee_bps: Decimal,
    base_slippage_bps: Decimal,
    k: Decimal,
) -> FillEvent:
    range_bps = (bar_high - bar_low) / next_open * Decimal("10000")
    slippage_bps = base_slippage_bps + k * range_bps
    direction = Decimal("1") if intent.side == Side.BUY else Decimal("-1")
    fill_price = next_open * (Decimal("1") + direction * slippage_bps / Decimal("10000"))
    fee = intent.quantity * fill_price * fee_bps / Decimal("10000")
    return FillEvent(
        event_id="sim-fill-1",
        symbol=intent.symbol,
        side=intent.side,
        quantity=intent.quantity,
        price=fill_price,
        fee=fee,
        ts_ms=0,
    )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src pytest tests/execution/test_sim_engine.py -v`  
Expected: PASS with `1 passed`

- [ ] **Step 5: Commit**

```bash
git add src/cta_core/execution/sim_engine.py tests/execution/test_sim_engine.py
git commit -m "feat: add simulated execution cost model"
```

### Task 8: Implement Risk Engine (M2/M3)

**Files:**
- Create: `src/cta_core/risk/engine.py`
- Create: `tests/risk/test_engine.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/risk/test_engine.py
from decimal import Decimal

from cta_core.risk.engine import RiskContext, RiskEngine


def test_risk_engine_rejects_daily_loss_breach():
    engine = RiskEngine(max_daily_loss=Decimal("500"))
    ctx = RiskContext(
        symbol="BTCUSDT",
        order_notional=Decimal("1000"),
        equity=Decimal("10000"),
        day_pnl=Decimal("-650"),
        losing_streak=1,
        symbol_notional=Decimal("2000"),
    )
    result = engine.check(ctx)
    assert result.allowed is False
    assert result.rule == "daily_max_loss"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest tests/risk/test_engine.py -v`  
Expected: FAIL with import error for risk engine

- [ ] **Step 3: Write minimal implementation**

```python
# src/cta_core/risk/engine.py
from dataclasses import dataclass
from decimal import Decimal


@dataclass(frozen=True)
class RiskContext:
    symbol: str
    order_notional: Decimal
    equity: Decimal
    day_pnl: Decimal
    losing_streak: int
    symbol_notional: Decimal


@dataclass(frozen=True)
class RiskResult:
    allowed: bool
    rule: str
    detail: str


class RiskEngine:
    def __init__(
        self,
        max_daily_loss: Decimal,
        max_losing_streak: int = 3,
        max_symbol_notional_ratio: Decimal = Decimal("0.4"),
    ):
        self.max_daily_loss = max_daily_loss
        self.max_losing_streak = max_losing_streak
        self.max_symbol_notional_ratio = max_symbol_notional_ratio

    def check(self, ctx: RiskContext) -> RiskResult:
        if abs(ctx.day_pnl) > self.max_daily_loss and ctx.day_pnl < 0:
            return RiskResult(False, "daily_max_loss", f"day_pnl={ctx.day_pnl}")
        if ctx.losing_streak >= self.max_losing_streak:
            return RiskResult(False, "losing_streak", f"losing_streak={ctx.losing_streak}")
        if ctx.symbol_notional / ctx.equity > self.max_symbol_notional_ratio:
            return RiskResult(False, "symbol_risk_budget", f"ratio={ctx.symbol_notional / ctx.equity}")
        return RiskResult(True, "pass", "ok")
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src pytest tests/risk/test_engine.py -v`  
Expected: PASS with `1 passed`

- [ ] **Step 5: Commit**

```bash
git add src/cta_core/risk/engine.py tests/risk/test_engine.py
git commit -m "feat: add core risk checks and rejection reasons"
```

### Task 9: Wire Backtest Runner with Replayable Event Log (M2)

**Files:**
- Create: `src/cta_core/app/backtest_runner.py`
- Create: `tests/integration/test_backtest_runner.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/integration/test_backtest_runner.py
import polars as pl

from cta_core.app.backtest_runner import run_backtest


def test_backtest_returns_replayable_events():
    bars = pl.DataFrame(
        {
            "open_time": [1, 2, 3, 4, 5, 6],
            "open": [10, 11, 12, 13, 14, 15],
            "high": [11, 12, 13, 14, 15, 16],
            "low": [9, 10, 11, 12, 13, 14],
            "close": [10, 11, 12, 13, 14, 15],
        }
    )
    output = run_backtest(bars=bars, symbol="BTCUSDT")
    assert len(output["events"]) > 0
    assert output["events"][0]["type"] == "BAR_CLOSED"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest tests/integration/test_backtest_runner.py -v`  
Expected: FAIL with import error for `backtest_runner`

- [ ] **Step 3: Write minimal implementation**

```python
# src/cta_core/app/backtest_runner.py
import polars as pl

from cta_core.events.models import EventType


def run_backtest(bars: pl.DataFrame, symbol: str) -> dict:
    events: list[dict] = []
    for row in bars.iter_rows(named=True):
        events.append(
            {
                "type": EventType.BAR_CLOSED.value,
                "symbol": symbol,
                "open_time": row["open_time"],
                "close": row["close"],
            }
        )
    return {"events": events}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src pytest tests/integration/test_backtest_runner.py -v`  
Expected: PASS with `1 passed`

- [ ] **Step 5: Commit**

```bash
git add src/cta_core/app/backtest_runner.py tests/integration/test_backtest_runner.py
git commit -m "feat: add replayable backtest event runner skeleton"
```

### Task 10: Implement Live Binance Adapter and Recovery Bootstrap (M3)

**Files:**
- Create: `src/cta_core/execution/live_binance.py`
- Create: `src/cta_core/app/live_runner.py`
- Create: `tests/execution/test_live_binance.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/execution/test_live_binance.py
from cta_core.execution.live_binance import LiveBinanceAdapter


def test_idempotency_key_is_stable():
    adapter = LiveBinanceAdapter(api_key="k", api_secret="s")
    key1 = adapter.client_order_id(strategy_id="sma", symbol="BTCUSDT", ts_ms=1000)
    key2 = adapter.client_order_id(strategy_id="sma", symbol="BTCUSDT", ts_ms=1000)
    assert key1 == key2
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest tests/execution/test_live_binance.py -v`  
Expected: FAIL with import error for `live_binance`

- [ ] **Step 3: Write minimal implementation**

```python
# src/cta_core/execution/live_binance.py
import hashlib


class LiveBinanceAdapter:
    BASE_URL = "https://fapi.binance.com"

    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret

    def client_order_id(self, strategy_id: str, symbol: str, ts_ms: int) -> str:
        raw = f"{strategy_id}|{symbol}|{ts_ms}".encode()
        return hashlib.sha256(raw).hexdigest()[:32]
```

```python
# src/cta_core/app/live_runner.py
from cta_core.execution.live_binance import LiveBinanceAdapter


def bootstrap_live_runner(api_key: str, api_secret: str) -> LiveBinanceAdapter:
    return LiveBinanceAdapter(api_key=api_key, api_secret=api_secret)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src pytest tests/execution/test_live_binance.py -v`  
Expected: PASS with `1 passed`

- [ ] **Step 5: Commit**

```bash
git add src/cta_core/execution/live_binance.py src/cta_core/app/live_runner.py tests/execution/test_live_binance.py
git commit -m "feat: add live binance adapter idempotency foundation"
```

### Task 11: Add Monitoring Signals and Alert Rules (M4)

**Files:**
- Create: `src/cta_core/ops/monitoring.py`
- Create: `tests/ops/test_monitoring.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/ops/test_monitoring.py
from cta_core.ops.monitoring import evaluate_alerts


def test_drawdown_alert():
    alerts = evaluate_alerts(drawdown_pct=0.09, submit_error_rate=0.01, ws_disconnects=0)
    assert "drawdown_breach" in alerts
```

- [ ] **Step 2: Run test to verify it fails**

Run: `PYTHONPATH=src pytest tests/ops/test_monitoring.py -v`  
Expected: FAIL with import error for monitoring module

- [ ] **Step 3: Write minimal implementation**

```python
# src/cta_core/ops/monitoring.py
def evaluate_alerts(drawdown_pct: float, submit_error_rate: float, ws_disconnects: int) -> list[str]:
    alerts: list[str] = []
    if drawdown_pct >= 0.08:
        alerts.append("drawdown_breach")
    if submit_error_rate >= 0.03:
        alerts.append("submit_error_spike")
    if ws_disconnects >= 3:
        alerts.append("ws_instability")
    return alerts
```

- [ ] **Step 4: Run test to verify it passes**

Run: `PYTHONPATH=src pytest tests/ops/test_monitoring.py -v`  
Expected: PASS with `1 passed`

- [ ] **Step 5: Commit**

```bash
git add src/cta_core/ops/monitoring.py tests/ops/test_monitoring.py
git commit -m "feat: add monitoring alert evaluation module"
```

### Task 12: Add CI Pipeline and Live Operation Runbook (M4)

**Files:**
- Create: `.github/workflows/ci.yml`
- Create: `docs/runbooks/live-operations.md`

- [ ] **Step 1: Write the failing test command**

Run: `PYTHONPATH=src pytest -q`  
Expected: Current baseline should fail if any previous task regressions were introduced.

- [ ] **Step 2: Add CI workflow**

```yaml
# .github/workflows/ci.yml
name: ci

on:
  push:
    branches: [main]
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - name: Install deps
        run: |
          python -m pip install -U pip
          pip install -e ".[dev]"
      - name: Run tests
        run: PYTHONPATH=src pytest -q
```

- [ ] **Step 3: Add live runbook**

```markdown
# docs/runbooks/live-operations.md

## Pre-Open Checklist
- Confirm API key permission scopes: futures trade/read.
- Confirm risk config: daily max loss, symbol budget, leverage cap.
- Confirm symbol whitelist matches deployment config.
- Confirm clock sync and UTC timestamps.

## Runtime Checks
- Order submit success rate >= 97%.
- Websocket disconnect count < 3 per hour.
- Drawdown does not exceed configured threshold.

## Incident Response
1. Trigger kill-switch (reduce-only mode).
2. Snapshot open positions and open orders.
3. Reconcile with exchange positions.
4. Restart live runner in recovery mode.
5. Resume only after metrics normalize.
```

- [ ] **Step 4: Run full validation**

Run: `PYTHONPATH=src pytest -q`  
Expected: PASS with all tests green.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/ci.yml docs/runbooks/live-operations.md
git commit -m "docs: add CI pipeline and live operations runbook"
```

## Verification Checklist Before Execution

- Confirm no task violates anti-lookahead (`DataPortal` gate only data source at runtime).
- Confirm all order/fill state transitions are append-only.
- Confirm live and backtest share the same event models.
- Confirm every task includes explicit tests and commit step.
- Confirm M1-M4 milestones each end with runnable, testable software.
