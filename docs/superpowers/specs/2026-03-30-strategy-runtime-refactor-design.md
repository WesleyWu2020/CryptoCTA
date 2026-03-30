# Strategy Runtime Refactor Design

Date: 2026-03-30

## Goal

Refactor the current single-script backtest entry pattern into a strategy-oriented runtime model where:

- each strategy lives in its own file and owns its own parameters and signal logic
- backtest and live execution share the same strategy interface
- runners handle data loading, execution, and output, not strategy-specific logic
- new strategies can be added without expanding one global CLI file

This design is intentionally incremental. It preserves current behavior where possible and avoids a large one-shot rewrite.

## Current Problem

The current backtest flow is centered around [`scripts/run_turtle_backtest.py`](/Users/dmiwu/work/PythonProject/CryptoCTA/scripts/run_turtle_backtest.py) and [`run_turtle_backtest(...)`](/Users/dmiwu/work/PythonProject/CryptoCTA/src/cta_core/app/turtle_backtest.py).

That flow has three responsibilities mixed together:

1. CLI parsing and parameter normalization
2. data loading and higher-timeframe wiring
3. strategy logic and execution behavior

`strategy_presets` improved parameter reuse, but presets are still only default-value containers. They are not true strategy modules. As more strategies are added, the current model will continue to accumulate:

- more CLI flags in one file
- more branching inside one large backtest function
- more coupling between research-only logic and future live-trading needs

## Design Choice

Three options were considered:

1. Keep one large backtest engine and only split parameter presets
2. Introduce a unified strategy class interface and move each strategy into its own module
3. Build a full plugin system with dynamic discovery and richer metadata

Chosen option: 2.

Reasoning:

- option 1 does not solve the core coupling problem
- option 3 is more abstraction than the current project needs
- option 2 gives a clean boundary between strategy logic and runtime logic while staying implementable within the current codebase

## Target Architecture

The refactor introduces three layers.

### 1. Strategy Layer

Each strategy is implemented in its own module and owns:

- its config dataclass
- its feature preparation logic
- its incremental decision logic
- its strategy metadata such as `strategy_id`

Examples:

- `src/cta_core/strategy_runtime/strategies/rp_daily_breakout.py`
- `src/cta_core/strategy_runtime/strategies/turtle_dual_tf.py`

### 2. Runtime Engine Layer

The engine is strategy-agnostic. It is responsible for:

- iterating over bars in chronological order
- passing lookahead-safe context to the strategy
- applying fills, fees, slippage, and position bookkeeping
- recording trades, equity, and summary metrics
- producing a stable backtest result contract

The engine should not know any RP-specific or turtle-specific business rules.

### 3. Runner Layer

Runners are thin orchestration entry points. They are responsible for:

- loading market data from DuckDB or Binance
- instantiating a chosen strategy
- selecting the correct runtime engine
- writing artifacts

Runners should not host large strategy parameter tables or strategy-specific branching.

## Unified Strategy Interface

The strategy interface is designed around incremental execution so the same strategy object can be reused for both backtest and live trading.

Core protocol:

- `on_start(context) -> None`
- `on_bar(context) -> list[StrategyDecision]`
- `on_finish(context) -> None`

Optional protocol for research efficiency:

- `prepare_features(bars, bars_htf=None) -> FeatureBundle`

The runtime calls `prepare_features(...)` once before replay, then drives the strategy bar by bar via `on_bar(...)`.

This preserves two requirements:

- live trading remains naturally event-driven
- batch backtests can still precompute features efficiently

## Core Runtime Objects

### StrategyConfig

Each strategy defines its own config dataclass. There should not be one giant shared config containing every possible parameter for every strategy.

Examples:

- `RPDailyBreakoutConfig`
- `TurtleDualTimeframeConfig`

This keeps each strategy's inputs explicit and limits cross-strategy parameter drift.

### StrategyContext

The engine passes a normalized context object on each bar. It should include:

- current timestamp and bar index
- current bar and optional higher-timeframe bar view
- lookback-safe feature view up to the current bar
- portfolio state such as cash, open position, and realized PnL
- runtime metadata needed by the strategy

The context must remain lookahead-safe. It must never expose future bars or future-derived features.

### StrategyDecision

Strategies emit normalized decisions rather than directly mutating engine state.

Minimum decision types:

- `ENTER_LONG`
- `EXIT_LONG`
- `ENTER_SHORT`
- `EXIT_SHORT`
- `HOLD`

The decision payload may also contain:

- target size or allocation
- optional stop or exit metadata
- reason code
- strategy tag for trade attribution

This keeps the execution path deterministic and testable.

## Proposed Directory Layout

```text
src/cta_core/strategy_runtime/
  base.py
  engine.py
  registry.py
  strategies/
    __init__.py
    rp_daily_breakout.py
    turtle_dual_tf.py

scripts/
  run_strategy_backtest.py
  run_rp_daily_breakout.py
  run_turtle_dual_tf.py
```

Notes:

- `run_strategy_backtest.py` is the generic entry point
- `run_<strategy>.py` scripts are optional convenience wrappers
- existing app-level runners can continue to exist as compatibility shims during migration

## Migration Strategy

The migration should be staged to avoid breaking the current workflow.

### Phase 1. Introduce runtime protocol and engine

Add the new strategy runtime primitives without moving strategy logic yet:

- `BaseStrategy`
- `StrategyContext`
- `StrategyDecision`
- `BacktestEngine`

At the end of this phase, the new engine can support the minimum replay loop, long-only positions, deterministic fees/slippage, and result generation.

### Phase 2. Move the current RP strategy into its own module

Extract the current RP-based logic into a first-class strategy module:

- `RPDailyBreakoutConfig`
- `RPDailyBreakoutStrategy`

The strategy should implement:

- `prepare_features(...)` for RP, regime, EMA, and confirmation state
- `on_bar(...)` for entry and exit decisions

At this stage, the old [`run_turtle_backtest(...)`](/Users/dmiwu/work/PythonProject/CryptoCTA/src/cta_core/app/turtle_backtest.py) may remain as a compatibility wrapper that internally delegates to the new runtime.

### Phase 3. Shrink legacy script responsibilities

Replace the role of [`scripts/run_turtle_backtest.py`](/Users/dmiwu/work/PythonProject/CryptoCTA/scripts/run_turtle_backtest.py) with a generic runner:

- `scripts/run_strategy_backtest.py`

The legacy script can either:

- remain as a compatibility entry point that forwards to the new runner, or
- be retired once downstream usage has moved

### Phase 4. Add additional strategies on the new interface

After the first strategy has stabilized, add further strategies as separate modules instead of extending a central parameter file.

## Interface Boundaries

The following boundaries must remain clear.

### Strategy modules own

- strategy-specific config
- feature engineering
- entry and exit conditions
- position sizing logic that depends on strategy signal quality

### Engine owns

- chronological replay
- execution price rules
- fees and slippage application
- position bookkeeping
- equity curve and metrics
- result serialization contract

### Runner owns

- CLI parsing
- data source selection
- strategy selection and config construction
- artifact output paths

If logic crosses these boundaries, maintainability will degrade back toward the current structure.

## Live Trading Compatibility

The primary reason to design around `on_bar(...)` is live reuse.

For live trading, the same strategy class should be callable from a live runner that receives new bars incrementally. The live runner should swap only:

- market data source
- order execution adapter
- operational safeguards

It should not require a separate reimplementation of signal logic.

This design reduces divergence risk between research, backtest, and live execution.

## Testing Strategy

Testing should be layered.

### Unit tests

Cover:

- strategy config validation
- feature-preparation correctness
- strategy decisions under small deterministic bar sequences
- engine fill and bookkeeping behavior

### Integration tests

Cover:

- replaying a known dataset through the new engine
- compatibility between old RP results and new RP strategy results
- CLI runner execution with a concrete `--strategy` value

### Regression tests

For the first migrated strategy, create a fixture-driven comparison to confirm the refactor does not materially change:

- trade count
- key actions
- total return
- max drawdown within a defined tolerance

## Acceptance Criteria

The refactor is successful when all of the following are true:

1. A generic runner can execute a strategy by `strategy_id`
2. The first RP strategy exists as an isolated strategy module
3. The runtime engine no longer contains RP-specific decision branches
4. New strategy modules can be added without modifying a giant shared CLI parameter list
5. The same strategy interface is suitable for both backtest and future live execution
6. Existing backtest behavior for the first migrated strategy remains materially consistent

## Non-Goals

This refactor does not aim to:

- build a full plugin marketplace or dynamic third-party loading model
- redesign all performance analytics or artifact formats at once
- migrate every existing strategy in a single step
- solve all live trading concerns in this change

The immediate goal is to establish a durable strategy/runtime boundary and migrate one real strategy through it.
