# Risk Layer Constraints

## Purpose
`cta_core.risk` enforces deterministic and explainable pre-trade checks.

## Hard Constraints
- Risk outputs must remain deterministic and explainable.
- Every rejection or gate decision must include stable `rule` and `detail` semantics.
- Risk decisions should not rely on non-deterministic state.

## Contracts
- Public risk interfaces are shared across runners/execution paths.
- Contract changes require synchronized updates in tests and callers.

## Validation Expectations
When editing this module:
- Run focused tests first: `PYTHONPATH=src pytest tests/risk -q`.
- Run full suite before completion: `PYTHONPATH=src pytest -q`.
- Add/update tests for changed rule behavior, including deterministic outcomes.
