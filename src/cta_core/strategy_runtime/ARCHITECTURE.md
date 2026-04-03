# Strategy Runtime Architecture

## Purpose
`strategy_runtime` defines strategy-facing contracts and executes strategy decisions in a strategy-agnostic loop.

## Main Components
- `base.py`: core contracts (`StrategyContext`, `StrategyDecision`, `BaseStrategy`) and position model.
- `engine.py`: backtest engine loop, fills, and accounting.
- `registry.py`: explicit mapping from strategy id to strategy factory.
- `interfaces.py` + `runtime.py`: lightweight bar-close strategy interfaces.
- `strategies/`: concrete implementations (for example `rp_daily_breakout`, `sma_cross`).

## Data/Control Flow
1. Runner resolves strategy id from CLI/preset.
2. Registry returns concrete strategy factory.
3. Engine iterates bars in ascending `open_time`.
4. Strategy receives context and emits decisions.
5. Engine applies execution/accounting rules and emits ordered events.

## Hard Boundaries
- Strategy-specific signal logic belongs in `strategies/`, not runners.
- `engine.py` must stay strategy-agnostic (no strategy-specific branching).
- Registration is explicit in `registry.py` (no dynamic auto-loading).
- Shared event contracts come from `cta_core.events.models`; avoid silent schema changes.

## Timing Semantics
- Any runtime change must keep timing explicit in tests/comments.
- If logic uses `signal_bar = t`, only data up to `t` close is allowed.
- Fill timing assumptions differ by path and must be documented in tests/output notes.

## Change Checklist
When editing this module:
- Update/add tests under `tests/strategy/`.
- Confirm anti-lookahead constraints remain true.
- Update this file if architecture or boundaries changed.
