# CryptoCTA

Local Python + Rust Binance USDT-M CTA MVP.

This repository contains an event-driven trading system skeleton with shared contracts for backtest and live paths. Python handles orchestration and strategy/risk/runtime logic, while Rust provides a ledger core binding.

## Current Scope

Implemented modules include:

- Config and package bootstrap (`cta_core.config`)
- Canonical event/order contracts (`cta_core.events`)
- Binance kline ingestion + parquet persistence (`cta_core.data`)
- Lookahead-safe data portal (`cta_core.data.data_portal`)
- Strategy runtime + SMA sample strategy (`cta_core.strategy_runtime`)
- Rust ledger binding bridge (`cta_core.bindings.ledger`)
- Simulated execution cost model (`cta_core.execution.sim_engine`)
- Core risk checks (`cta_core.risk`)
- Replayable backtest event skeleton (`cta_core.app.backtest_runner`)
- Live adapter idempotency skeleton (`cta_core.execution.live_binance`)
- Monitoring alert evaluator (`cta_core.ops.monitoring`)
- CI workflow and live operations runbook

The codebase is an MVP foundation, not a production-complete trading system.

## Requirements

- Python 3.12+
- Rust toolchain (`rustc`, `cargo`)
- `maturin` (for Rust extension development)

## Quick Start

```bash
python -m pip install -U pip
pip install -e ".[dev]"
```

Run tests:

```bash
PYTHONPATH=src pytest -q
```

## Rust Ledger Binding

The Python binding imports `cta_ledger`. Build/install the extension locally before ledger-specific runs:

```bash
maturin develop --manifest-path rust/ledger_core/Cargo.toml
```

Then run:

```bash
PYTHONPATH=src pytest tests/bindings/test_ledger_binding.py -q
```

## Repository Layout

```text
src/cta_core/
  app/               # backtest/live runner skeletons
  bindings/          # Python wrappers for Rust extensions
  config/            # runtime settings models
  data/              # ingestion, storage, and anti-lookahead access
  events/            # canonical domain contracts
  execution/         # sim execution + live adapter skeleton
  ops/               # monitoring and alert checks
  risk/              # pre-trade risk engine
  strategy_runtime/  # strategy interface/runtime/sample strategy

rust/ledger_core/    # Rust PyO3 ledger extension
tests/               # unit/integration tests by domain
docs/runbooks/       # operational documentation
```

## Operations

- CI: [`.github/workflows/ci.yml`](/Users/dmiwu/work/PythonProject/CryptoCTA/.github/workflows/ci.yml)
- Live runbook: [`docs/runbooks/live-operations.md`](/Users/dmiwu/work/PythonProject/CryptoCTA/docs/runbooks/live-operations.md)

