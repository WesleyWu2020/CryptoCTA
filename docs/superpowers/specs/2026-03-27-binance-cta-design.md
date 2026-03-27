# Binance USDT-M CTA System Design (MVP)

Date: 2026-03-27  
Scope: Local crypto CTA system with backtesting and live trading, built for Binance USDT-M perpetual futures.

## 1. Goals and Boundaries

### Goals
- Build one local system that supports both backtest and live execution with consistent semantics.
- Support 15m and 1h bar-frequency CTA trading.
- Prevent common quantitative errors:
  - no look-ahead / future-data leakage
  - explicit slippage and trading cost modeling
  - replayable and auditable event flows
- Run live on Binance mainnet with controlled risk.

### Fixed choices (confirmed)
- Exchange instrument: Binance USDT-M perpetual only
- Symbols (MVP): small fixed set (e.g., BTCUSDT, ETHUSDT, SOLUSDT)
- Storage: Parquet + DuckDB
- Strategy scope: single strategy, single portfolio
- Trigger mode: bar-close only (15m/1h)
- Timezone: UTC end-to-end
- Order types (MVP): market + limit
- Risk level: medium
- Language stack: Python + Rust
  - Rust only for matching/accounting core
  - Python for orchestration, data, strategy, risk, exchange adapter

### Out of scope (MVP)
- Coin-M and delivery contracts
- Multi-strategy and multi-account orchestration
- Intrabar triggers
- Advanced order types (trailing stop, iceberg, etc.)

## 2. High-Level Architecture

## 2.1 Components
- `data-service` (Python)
  - Pulls Binance klines (REST for history + incremental updates)
  - Writes normalized Parquet tables
  - Exposes DuckDB query layer for research/runtime
- `strategy-runtime` (Python)
  - Runs only on `bar-close` events
  - Reads only approved closed-bar/features views
  - Outputs `target_position` or `order_intent`
- `risk-engine` (Python)
  - Enforces leverage cap, symbol risk budget, daily loss limit, losing-streak circuit breaker
  - Can trigger global kill-switch
- `execution-sim` (Python + Rust core)
  - Simulated order processing and fills using the same event schema as live
- `execution-live` (Python)
  - Binance order adapter (market/limit)
  - Maps exchange callbacks to internal fill/order events
- `portfolio-ledger` (Rust core + Python binding)
  - Order lifecycle transitions, position updates, realized/unrealized PnL
  - Fees, funding, and equity accounting

## 2.2 Strategy placement
CTA strategy code must live in `strategy-runtime` only, not in risk or execution modules.

Recommended layout:
- `src/strategy_runtime/strategies/<strategy_name>/`
- `src/strategy_runtime/interfaces.py` (strategy interface)
- `config/strategies/<strategy_name>.yaml` (strategy params)

This keeps strategy replacement isolated from execution and accounting internals.

## 3. Data Model and Anti-Lookahead Design

## 3.1 Data layers
- `raw_klines`
  - Exchange-native fields (symbol, interval, open_time, close_time, ohlcv, trade_count, etc.)
- `bars_closed`
  - Canonical closed bars only
  - Only approved market data source for strategy runtime
- `features`
  - Derived indicators built from `bars_closed`
  - Must use only information available at decision time

Primary key recommendation:
- `(symbol, interval, open_time)` in UTC

## 3.2 Hard anti-lookahead rules
- Strategy runtime never receives full unrestricted historical tables.
- Strategy can only access `<= t` closed-bar view at event time `t`.
- Default feature construction must apply lag discipline (for example, shift-based usage).
- All strategy data access must go through a controlled `DataPortal` API.
- Any request for `> t` data raises an exception and writes an audit record.

## 3.3 Signal-to-fill timing contract
- Signal generated at bar-close `t` can be filled no earlier than next bar open `t+1` (default model).
- Any alternative fill convention must be explicit and separately labeled.

## 4. Unified Event Flow and State Machines

## 4.1 Event flow (backtest/live shared semantics)
`BarClosed -> SignalGenerated -> RiskChecked -> OrderIntentCreated -> OrderSubmitted -> FillReceived -> PortfolioUpdated -> MetricsUpdated`

Difference between backtest and live is only event source at execution step:
- Backtest: simulated fills from `execution-sim` + Rust core
- Live: Binance acks/fills mapped by `execution-live`

## 4.2 Order state machine
- `NEW -> RISK_REJECTED | SENT -> ACKED -> PARTIAL_FILLED -> FILLED`
- Exceptional paths:
  - `SENT/ACKED -> CANCELED | REJECTED | EXPIRED`

Rules:
- State transitions are append-only in `order_events`.
- Idempotency keys:
  - internal: `client_order_id`
  - exchange: `exchange_order_id`
- Repeated callbacks must not double-apply fills.

## 4.3 Portfolio/accounting state
- Positions update only on `FillReceived`.
- Equity/PnL snapshots generated per bar-close.
- Replay must rebuild the same account state from event logs.
- Restart procedure must recover open orders/positions before resuming.

## 5. Execution Cost Model and Risk Controls

## 5.1 Slippage model (MVP)
Parameterizable directional slippage:
- `slippage_bps = base_bps + k * (high - low) / close * 10000`
- Buy increases effective price; sell decreases effective price.

Limit-order simulation:
- Fill only if next-bar range touches limit price.
- If not touched, default behavior is cancel at bar end (MVP default).

## 5.2 Cost and PnL decomposition
- Fees: maker/taker configuration
- Funding: charged at funding timestamps on notional exposure
- Report decomposition:
  - `gross_pnl`, `fees`, `funding`, `slippage`, `net_pnl`

## 5.3 Medium risk controls (confirmed)
- Leverage cap check (post-trade estimate)
- Per-symbol risk budget cap
- Daily max loss kill-switch (allow reduce-only after trigger)
- Losing-streak circuit breaker with cooldown

Risk execution order:
`Signal -> PositionSizing -> RiskCheck -> OrderIntent`

Risk rejection must record:
- rule name
- threshold
- observed value
- timestamp/context

## 6. Observability and Operations

Minimum monitoring alerts:
- order submission failure rate
- fill/ack latency spikes
- risk-trigger activations
- daily drawdown threshold breach
- exchange stream disconnect/reconnect anomalies

Initial alert channels:
- local logs + one external channel (Feishu or Telegram)

## 7. Testing Strategy

## 7.1 Unit tests
- Rust ledger core:
  - order transitions, partial fills, accounting correctness
- Python risk engine:
  - leverage/risk-budget/daily-loss/losing-streak boundary conditions

## 7.2 Integration tests
- Fixed market + fixed signal inputs produce deterministic replayable results.
- Exchange adapter handles timeout/reject/disconnect recovery paths.

## 7.3 Anti-cheat tests
- Intentional future-data request must fail and be audited.
- Signal/fill temporal contract checks (`t` signal, earliest `t+1` fill).

## 8. Delivery Milestones

- `M1` Data foundation
  - USDT-M 15m/1h ingestion, Parquet lake, DuckDB query, data validation
- `M2` Research/backtest loop
  - single strategy with slippage/fees/funding and equity metrics
- `M3` Bridge to live
  - unified event flow + risk engine + Binance adapter (small capital deployment)
- `M4` Mainnet hardening
  - resiliency/recovery, monitoring/alerts, runbook, parameter freeze

## 9. Definition of Done (MVP)

- Backtest and live share event semantics and accounting core.
- Anti-lookahead guard is enforced and test-covered.
- Live runner can recover from restart/disconnect with consistent state.
- Risk controls are enforced pre-trade and are auditable via event logs.
