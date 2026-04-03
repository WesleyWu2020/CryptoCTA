# Execution Layer Constraints

## Purpose
`cta_core.execution` provides simulated and live order execution adapters used by backtest and live runners.

## Hard Constraints
- Shared order/fill interfaces come from `cta_core.events.models`; avoid silent contract changes.
- `LiveBinanceAdapter.client_order_id(...)` must remain deterministic for idempotency.
- Live submit path remains MARKET-only unless explicitly expanded with tests and docs updates.
- Quantity/signature formatting must stay deterministic and API-compatible.

## Simulation vs Live
- `sim_engine.py` is for deterministic simulation behavior.
- `live_binance.py` is for exchange I/O and request signing.
- Do not mix exchange-specific behavior into simulation internals.

## Validation Expectations
When editing this module:
- Run focused tests first: `PYTHONPATH=src pytest tests/execution -q`.
- Run full suite before completion: `PYTHONPATH=src pytest -q`.
- If request/signing/client-id logic changes, add/update submit-path tests.
