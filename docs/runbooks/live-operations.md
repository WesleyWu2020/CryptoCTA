# Live Operations Runbook

## Pre-Open Checklist

- Confirm API scopes are limited to the minimum required trading permissions.
- Verify risk config is loaded and matches the approved live limits.
- Confirm the symbol whitelist contains only instruments approved for live trading.
- Check clock sync on the trading host before market open.

## Runtime Checks

- Submit success rate is at least 97%.
- Websocket disconnect rate stays below 3 per hour.
- Drawdown remains under the configured threshold.

## Incident Response

1. Trigger kill-switch (reduce-only mode).
2. Snapshot open positions and open orders.
3. Reconcile with exchange positions.
4. Restart live runner in recovery mode.
5. Resume only after metrics normalize.
