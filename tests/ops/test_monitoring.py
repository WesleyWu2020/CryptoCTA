from cta_core.ops.monitoring import evaluate_alerts


def test_drawdown_alert():
    alerts = evaluate_alerts(drawdown_pct=0.09, submit_error_rate=0.01, ws_disconnects=0)
    assert "drawdown_breach" in alerts


def test_drawdown_alert_at_exact_boundary():
    alerts = evaluate_alerts(drawdown_pct=0.08, submit_error_rate=0.01, ws_disconnects=0)
    assert "drawdown_breach" in alerts


def test_submit_error_alert():
    alerts = evaluate_alerts(drawdown_pct=0.01, submit_error_rate=0.03, ws_disconnects=0)
    assert "submit_error_spike" in alerts


def test_ws_disconnect_alert():
    alerts = evaluate_alerts(drawdown_pct=0.01, submit_error_rate=0.01, ws_disconnects=3)
    assert "ws_instability" in alerts


def test_no_alerts_below_thresholds():
    alerts = evaluate_alerts(drawdown_pct=0.079, submit_error_rate=0.029, ws_disconnects=2)
    assert alerts == []


def test_multiple_alerts_can_emit_together():
    alerts = evaluate_alerts(drawdown_pct=0.08, submit_error_rate=0.03, ws_disconnects=3)
    assert set(alerts) == {"drawdown_breach", "submit_error_spike", "ws_instability"}
