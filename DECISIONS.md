# Design Decisions

## 2026-04-03: Use explicit strategy registry (no dynamic auto-discovery)
- Reason: The strategy list stays auditable and testable, and avoids runtime implicit loading behavior.
- Rejected alternative: Directory scan + reflection-based auto-registration (higher debugging and change-risk cost).
- Constraint: Every new strategy must be explicitly registered in `cta_core.strategy_runtime.registry` with matching tests.

## 2026-04-03: Keep DataPortal strictly no-lookahead
- Reason: Backtest validity requires that signals never read future data.
- Rejected alternative: Future-derived backfilling to "complete" features (for example `bfill` on signal columns), which contaminates historical decisions.
- Constraint: Future-leak patterns are forbidden (`shift(-1)`, `pct_change(-1)`, `center=True`, `iloc[i+1]`); timing/alignment changes must include anti-lookahead tests.

## 2026-04-03: Keep `signal_bar` and `fill_bar` timing explicit
- Reason: Signal time and fill time assumptions are foundational for reproducible, interpretable backtest results.
- Rejected alternative: Implicit or ambiguous fill timing (for example filling at signal-bar extremes by default), which introduces optimistic bias.
- Constraint: `app/strategy_backtest/execution.py` path uses `fill_bar=t+1`; `strategy_runtime/engine.py` currently assumes current-bar close fills and must be documented in tests/output notes when used.

## 2026-04-03: Keep replay output contract fixed as `{"events": [...]}`
- Reason: A stable replay schema simplifies downstream consumption, reconciliation, and regression comparison.
- Rejected alternative: Different output structures per execution path (higher integration and validation cost).
- Constraint: Event records remain ordered; contract changes require synchronized caller and test updates.

## 2026-04-03: Keep Live Binance `client_order_id` deterministic
- Reason: Live retry/idempotency handling depends on stable client order ids.
- Rejected alternative: Randomized or jittered ids (harder deduplication and traceability on retries).
- Constraint: Any id-generation change must update submit-path tests and operations notes.
