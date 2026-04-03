# Live Operations Runbook

## RP Daily Breakout Launch Checklist

1. Confirm the deployment parameters before market open:
   - Strategy: `rp_daily_breakout`
   - Symbol: `BTCUSDT`
   - Interval: `1h`
   - State file: `artifacts/live_state/rp_daily_breakout.json`
   - Risk defaults: `--max-daily-loss 500 --max-losing-streak 3 --max-symbol-notional-ratio 0.4 --max-leverage 1 --fee-bps 5`
2. Run the dry-run launch command for at least 30 minutes and confirm there are no unexpected decisions or repeated state rewinds:

```bash
PYTHONPATH=src python scripts/run_live_strategy.py \
  --strategy rp_daily_breakout \
  --symbol BTCUSDT \
  --interval 1h \
  --state-path artifacts/live_state/rp_daily_breakout.json \
  --max-daily-loss 500 \
  --max-losing-streak 3 \
  --max-symbol-notional-ratio 0.4 \
  --max-leverage 1 \
  --fee-bps 5 \
  --dry-run
```

3. Verify the state file advances after a new closed bar is processed:

```bash
cat artifacts/live_state/rp_daily_breakout.json
```

4. Start the live runner with explicit strategy and market arguments from the plan:

```bash
PYTHONPATH=src python scripts/run_live_strategy.py \
  --strategy rp_daily_breakout \
  --symbol BTCUSDT \
  --interval 1h \
  --state-path artifacts/live_state/rp_daily_breakout.json \
  --max-daily-loss 500 \
  --max-losing-streak 3 \
  --max-symbol-notional-ratio 0.4 \
  --max-leverage 1 \
  --fee-bps 5 \
  --api-key "$BINANCE_API_KEY" \
  --api-secret "$BINANCE_API_SECRET"
```

5. After the first live cycle, verify:
   - The first accepted order has the expected deterministic client order identity on the exchange side.
   - The state file `last_submit_ts_ms` matches the bar timestamp used for the latest non-error live submission outcome.
   - No `live_runner alerts=` line is printed.

## Runtime Checks

- Investigate immediately if `live_runner alerts=` prints any of:
  - `drawdown_breach`
  - `submit_error_spike`
- Treat the runtime as healthy only when all of the following remain true:
  - Drawdown is below `8%`
  - Submit error rate is below `3%`
- `ws_instability` is reserved for websocket-backed runners. This REST polling loop does not currently increment `ws_disconnects`, so that alert is not expected to fire from `scripts/run_live_strategy.py` today.

## Incident Rollback Checklist

1. Stop the live runner process and prevent further submissions.
2. Snapshot the current exchange state:
   - Open positions
   - Open orders
   - Most recent accepted order timestamp
3. Snapshot the local recovery state file before any restart:

```bash
cat artifacts/live_state/rp_daily_breakout.json
```

4. Reconcile exchange state against local state:
   - `last_processed_open_time` must not move backward.
   - `last_submit_ts_ms` must match the latest non-error live submission outcome (fresh acceptance or recognized idempotent duplicate), or be corrected before restart.
   - Any unexpected live position or order must be flattened or canceled before resuming automation.
5. Restart in dry-run first if the exchange and local state were out of sync:

```bash
PYTHONPATH=src python scripts/run_live_strategy.py \
  --strategy rp_daily_breakout \
  --symbol BTCUSDT \
  --interval 1h \
  --state-path artifacts/live_state/rp_daily_breakout.json \
  --fee-bps 5 \
  --dry-run
```

6. Resume live trading only when all rollback clear conditions are met:
   - Exchange positions and open orders match the intended strategy state.
   - The state file reflects the reconciled `last_processed_open_time` and `last_submit_ts_ms`.
   - `live_runner alerts=` is no longer emitted.
   - Alert thresholds have cleared: drawdown below `8%` and submit error rate below `3%`.
   - If a websocket disconnect counter is added in a future runner revision, verify `ws_disconnects` is below `3` before treating `ws_instability` as cleared.
