def evaluate_alerts(drawdown_pct: float, submit_error_rate: float, ws_disconnects: int) -> list[str]:
    alerts: list[str] = []
    if drawdown_pct >= 0.08:
        alerts.append("drawdown_breach")
    if submit_error_rate >= 0.03:
        alerts.append("submit_error_spike")
    if ws_disconnects >= 3:
        alerts.append("ws_instability")
    return alerts
