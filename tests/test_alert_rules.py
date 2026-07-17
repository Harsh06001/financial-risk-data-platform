import pytest

from alerts.evaluate_alerts import evaluate_observations


def test_only_nonpassing_observations_at_threshold_alert():
    observations = [
        {"status": "PASS", "severity": "CRITICAL", "metric_name": "pass"},
        {"status": "WARN", "severity": "INFO", "metric_name": "info"},
        {"status": "WARN", "severity": "WARNING", "metric_name": "warn"},
        {"status": "FAIL", "severity": "CRITICAL", "metric_name": "fail"},
    ]
    alerts = evaluate_observations(observations, "WARNING")
    assert [item["metric_name"] for item in alerts] == ["warn", "fail"]


def test_critical_threshold_filters_warnings():
    observations = [
        {"status": "WARN", "severity": "WARNING"},
        {"status": "FAIL", "severity": "CRITICAL"},
    ]
    assert len(evaluate_observations(observations, "CRITICAL")) == 1


def test_unknown_minimum_severity_is_rejected():
    with pytest.raises(ValueError):
        evaluate_observations([], "URGENT")
