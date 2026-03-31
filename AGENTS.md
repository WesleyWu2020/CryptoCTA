# AGENTS.md

Guidance for AI/human coding agents working in this repository.

## 1) Current Code Structure (Re-baselined)

### 1.1 Layered Architecture

1. Runner/Orchestration layer (`src/cta_core/app/`)
- `strategy_backtest/`: generic strategy backtest entry (`main`), CLI parsing, execution routing, data-source selection.
- `strategy_presets/`: named preset defaults (`BacktestStrategyPreset`) for runnable strategies.
- `turtle_backtest.py`: compatibility backtest pipeline used by current RP execution path.
- `backtest_runner.py`: minimal replay contract (`{"events": [...]}`) for event-sequence outputs.
- `live_runner.py`: bootstrap for live adapter wiring.

2. Strategy Runtime layer (`src/cta_core/strategy_runtime/`)
- `base.py`: core runtime contracts (`StrategyContext`, `StrategyDecision`, `BaseStrategy`, position model).
- `engine.py`: strategy-agnostic backtest engine and fill/accounting loop.
- `registry.py`: strategy id -> factory mapping.
- `interfaces.py` + `runtime.py`: lightweight bar-close strategy interface support.
- `strategies/`: concrete strategy implementations (`rp_daily_breakout`, `sma_cross`).

3. Domain/Infrastructure layer (`src/cta_core/`)
- `data/`: Binance ingest, normalization, DuckDB upsert/read, lookahead-safe `DataPortal`.
- `events/`: shared event/order/fill contracts.
- `execution/`: simulated fill model + live Binance adapter.
- `risk/`: deterministic risk rules and explainable risk results.
- `ops/`: monitoring alert checks.
- `config/`: pydantic runtime settings.
- `bindings/`: Python bridge for Rust ledger extension.

4. Entry Scripts (`scripts/`)
- `run_strategy_backtest.py`: canonical CLI entry for registered strategies.
- `run_turtle_backtest.py`: compatibility wrapper that forwards to generic strategy runner.
- Other scripts (`ingest_klines_to_duckdb.py`, `walkforward_turtle.py`, `ablation_single_factor.py`) are research/ops utilities.

### 1.2 Boundary Rules

- Keep strategy-specific signal logic inside `strategy_runtime/strategies/`, not in runners.
- Keep runner code focused on argument parsing, data loading, strategy selection, and output writing.
- Keep engine code strategy-agnostic (no RP-only branching inside generic runtime abstractions).
- Treat `events.models` as shared contracts across backtest/live paths.

## 2) Working Rules

- Keep changes task-scoped and minimal.
- Preserve module boundaries described above.
- Prefer explicit, test-backed behavior changes over broad refactors.
- Do not remove or rewrite user-authored work unless explicitly requested.
- Avoid destructive git operations (`reset --hard`, force checkout, etc.) unless explicitly approved.

## 3) Quality Gates

Before claiming completion, run:

```bash
PYTHONPATH=src pytest -q
```

If you touch Rust ledger bindings, also run:

```bash
maturin develop --manifest-path rust/ledger_core/Cargo.toml
PYTHONPATH=src pytest tests/bindings/test_ledger_binding.py -q
```

Recommended focused test scopes before full suite:

- `tests/strategy/` for `strategy_runtime/*`
- `tests/integration/test_run_strategy_backtest.py` for runner/script behavior
- `tests/data/` for ingest/store/portal changes
- `tests/execution/`, `tests/risk/`, `tests/events/`, `tests/ops/` for domain modules

## 4) Project-Specific Constraints

- Event contracts in `cta_core.events.models` are shared interfaces; avoid silent breaking changes.
- `DataPortal` must remain lookahead-safe (no future-bar access).
- Risk checks should remain deterministic and explainable (`rule` + `detail`).
- Backtest replay output contract is `{"events": [...]}` with ordered event records.
- `LiveBinanceAdapter.client_order_id(...)` is intentionally deterministic for idempotency.
- Strategy registration must stay explicit via `cta_core.strategy_runtime.registry` (no implicit dynamic loading).

## 5) File/Artifact Hygiene

Do not commit generated artifacts such as:

- `__pycache__/`
- `build/`
- `rust/ledger_core/target/`
- `*.egg-info/`

If these appear locally, leave them untracked unless the user asks otherwise.

## 6) Preferred Workflow

1. Read relevant module + tests first.
2. Add or update tests for the intended behavior.
3. Implement the minimal code change.
4. Run focused tests, then full suite.
5. Commit with a clear, scoped message.
