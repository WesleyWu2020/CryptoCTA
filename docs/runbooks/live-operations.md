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

1. Pause new order submission.
2. Inspect recent logs, metrics, and exchange status.
3. Reconcile open orders and current positions.
4. Apply the smallest safe recovery action, such as reconnecting the websocket or restarting the runner.
5. Resume trading only after the runtime checks are back within limits.
