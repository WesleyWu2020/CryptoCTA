# AGENTS.md

Landing page for AI/human agents in this repo.

## What This Project Is
- Crypto CTA framework with shared contracts for backtest and live trading.
- Core domains live in `src/cta_core/`: data, events, execution, risk, ops, strategy runtime.
- Main usage is strategy backtesting via CLI scripts and runtime engine.

## Quick Structure
- Runner/orchestration: `src/cta_core/app/`
  - `strategy_backtest/`, `strategy_presets/`, `backtest_runner.py`, `live_runner.py`
- Strategy runtime: `src/cta_core/strategy_runtime/`
  - `base.py`, `engine.py`, `registry.py`, `strategies/`
- Entry scripts:
  - `scripts/run_strategy_backtest.py` (canonical)
  - `scripts/run_turtle_backtest.py` (compat wrapper)

## How To Run
- Install deps:

```bash
make setup
```

- Run a strategy backtest (canonical script):

```bash
PYTHONPATH=src python scripts/run_strategy_backtest.py --help
```

- Turtle compatibility path:

```bash
PYTHONPATH=src python scripts/run_turtle_backtest.py --help
```

## How To Validate
- Standard checks:

```bash
make check
```

- Required test gate before claiming completion:

```bash
PYTHONPATH=src pytest -q
```

- If Rust ledger bindings were touched:

```bash
maturin develop --manifest-path rust/ledger_core/Cargo.toml
PYTHONPATH=src pytest tests/bindings/test_ledger_binding.py -q
```

- Recommended focused scopes first:
  - `PYTHONPATH=src pytest tests/strategy -q`
  - `PYTHONPATH=src pytest tests/integration/test_run_strategy_backtest.py -q`

## Critical Rules (Do Not Break)
- Keep strategy logic inside `src/cta_core/strategy_runtime/strategies/`.
- Keep runners focused on parsing, loading, routing, and output.
- Keep `strategy_runtime/engine.py` strategy-agnostic.
- Treat `cta_core.events.models` as shared contracts; avoid silent breaking changes.
- Keep `DataPortal` lookahead-safe (no future-bar access).
- Keep risk outputs deterministic and explainable (`rule` + `detail`).
- Strategy registration is explicit via `cta_core.strategy_runtime.registry`.
- Keep module docs near code and update them with related code changes:
  - `src/cta_core/strategy_runtime/ARCHITECTURE.md`
  - `src/cta_core/data/CONSTRAINTS.md`
  - `src/cta_core/execution/CONSTRAINTS.md`
  - `src/cta_core/risk/CONSTRAINTS.md`
- CI enforces doc drift for these modules via `make docs-drift-check`.

## No-Lookahead / Timing Rules
- Define timing semantics in code comments/tests: `signal_bar` vs `fill_bar`.
- `signal_bar = t`: signals may only use data up to bar `t` close.
- For app strategy backtest execution path, fills are `fill_bar = t+1`.
- Do not introduce future leaks (`shift(-1)`, `pct_change(-1)`, `center=True`, `iloc[i+1]`, `bfill` on signal columns).
- Any timing/exit/MTF alignment change must include tests, including at least one anti-lookahead test.

## Working Style
- Keep changes minimal and task-scoped.
- Prefer explicit, test-backed behavior changes over broad refactors.
- Do not rewrite/remove user-authored work unless asked.
- Avoid destructive git operations unless explicitly approved.
- Record important architecture/constraint decisions in `DECISIONS.md`.

## More Details
- Full detailed handbook moved to: `docs/dev/agent-rules.md`
