# Data Layer Constraints

## Purpose
`cta_core.data` is responsible for ingest, normalization, storage, and lookahead-safe access.

## Hard Constraints
- `DataPortal` must be lookahead-safe: never expose future-bar data to current decisions.
- Bars must be processed in ascending `open_time`.
- Multi-timeframe joins may only use closed higher-timeframe bars (`htf_close_time <= signal_time`).
- Do not use future-leak transforms in signal feature generation:
  - `shift(-1)`
  - `pct_change(-1)`
  - `rolling(..., center=True)`
  - `iloc[i+1]`
  - `bfill` on signal columns

## Contracts
- Input/output schemas used by runners/runtime must remain explicit and stable.
- Any contract change must be reflected in tests and calling modules.

## Validation Expectations
When editing this module:
- Run focused data tests first: `PYTHONPATH=src pytest tests/data -q`.
- Run full suite before completion: `PYTHONPATH=src pytest -q`.
- Add/update at least one anti-lookahead test when timing/feature alignment changes.

## Hygiene
- Keep transformations deterministic.
- Prefer explicit column naming and stable ordering.
- Document timing assumptions near code and tests.
