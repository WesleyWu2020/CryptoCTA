# Progress

Last Updated: 2026-04-03
Owner: @dmiwu

## Current Sprint Goal
- Standardize agent landing docs and bind architecture knowledge updates to code changes.

## Done
- Added module docs near code:
- `src/cta_core/strategy_runtime/ARCHITECTURE.md`
- `src/cta_core/data/CONSTRAINTS.md`
- Added `Makefile` with `setup`, `test`, `lint`, `docs-check`, `check`.
- CI now runs `docs-check` + `lint` before tests.
- Added strict CI docs drift gate (`docs-drift-check`) based on diff range (`BASE_SHA..HEAD_SHA`).
- Extended module-doc drift enforcement to `execution/` and `risk/`.
- Added module docs:
- `src/cta_core/execution/CONSTRAINTS.md`
- `src/cta_core/risk/CONSTRAINTS.md`
- Moved full agent handbook to `docs/dev/agent-rules.md`; root `AGENTS.md` remains short landing page.

## In Progress
- Local environment dependency bootstrap for full `make check` test stage.

## Next
- Extend docs drift mapping to additional modules as local docs are added.

## Blocked / Risks
- `docs-check` currently verifies required files exist, not semantic freshness.
- Local test run currently blocked by missing packages/modules: `polars`, `duckdb`, `cta_ledger`.

## Validation
- Planned:
  - `PYTHONPATH=src pytest -q`
- Focused (optional before full suite):
  - `PYTHONPATH=src pytest tests/strategy -q`
  - `PYTHONPATH=src pytest tests/integration/test_run_strategy_backtest.py -q`

## Notes
- Keep entries concise and task-scoped.
- Prefer outcome + evidence (what changed, where, test status).
- If timing semantics changed, explicitly note `signal_bar` and `fill_bar` assumptions.
