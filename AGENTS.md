# AGENTS.md

Guidance for AI/human coding agents working in this repository.

## 1) Working Rules

- Keep changes task-scoped and minimal.
- Preserve existing module boundaries under `src/cta_core/`.
- Prefer explicit, test-backed behavior changes over broad refactors.
- Do not remove or rewrite user-authored work unless explicitly requested.
- Avoid destructive git operations (`reset --hard`, force checkout, etc.) unless explicitly approved.

## 2) Quality Gates

Before claiming completion, run:

```bash
PYTHONPATH=src pytest -q
```

If you touch Rust ledger bindings, also run:

```bash
maturin develop --manifest-path rust/ledger_core/Cargo.toml
PYTHONPATH=src pytest tests/bindings/test_ledger_binding.py -q
```

## 3) Project-Specific Constraints

- Event contracts in `cta_core.events.models` are shared interfaces; avoid silent breaking changes.
- `DataPortal` must remain lookahead-safe (no future-bar access).
- Risk checks should remain deterministic and explainable (`rule` + `detail`).
- Backtest replay output contract is `{"events": [...]}` with ordered event records.
- `LiveBinanceAdapter.client_order_id(...)` is intentionally deterministic for idempotency.

## 4) File/Artifact Hygiene

Do not commit generated artifacts such as:

- `__pycache__/`
- `build/`
- `rust/ledger_core/target/`
- `*.egg-info/`

If these appear locally, leave them untracked unless the user asks otherwise.

## 5) Preferred Workflow

1. Read relevant module + tests first.
2. Add or update tests for the intended behavior.
3. Implement the minimal code change.
4. Run focused tests, then full suite.
5. Commit with a clear, scoped message.

