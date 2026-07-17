import json

from monitoring.alert_webhook import normalize_alerts, persist_and_emit
from monitoring.metrics_exporter import collect_metric_values, render_prometheus


def test_metrics_exporter_aggregates_streaming_and_optional_status(tmp_path):
    metrics_root = tmp_path / "metrics"
    metrics_root.mkdir()
    (metrics_root / "batch_id=0.json").write_text(
        json.dumps(
            {
                "input_count": 10,
                "clean_count": 7,
                "invalid_count": 2,
                "duplicate_count": 1,
                "late_count": 1,
                "schema_drift_count": 1,
                "dlq_count": 2,
                "risk_alert_count": 3,
                "reconciliation": True,
                "processing_timestamp": "2026-07-16T12:00:00+00:00",
                "processing_duration_seconds": 2.5,
            }
        ),
        encoding="utf-8",
    )
    bigquery = tmp_path / "bigquery.json"
    bigquery.write_text(json.dumps({"success": False}), encoding="utf-8")
    dbt = tmp_path / "run_results.json"
    dbt.write_text(
        json.dumps(
            {
                "results": [
                    {"unique_id": "test.project.streaming_quality", "status": "fail"},
                    {"unique_id": "test.project.batch_quality", "status": "pass"},
                ]
            }
        ),
        encoding="utf-8",
    )

    values = collect_metric_values(metrics_root, bigquery, dbt)
    assert values["streaming_invalid_record_ratio"] == 0.2
    assert values["streaming_dlq_events_total"] == 2
    assert values["streaming_bigquery_load_success"] == 0
    assert values["streaming_dbt_test_failures"] == 1
    rendered = render_prometheus(values)
    assert "streaming_last_batch_timestamp_seconds" in rendered
    assert "streaming_reconciliation_failures_total 0" in rendered


def test_missing_generated_artifacts_are_exported_as_unavailable(tmp_path):
    values = collect_metric_values(
        tmp_path / "missing-metrics",
        tmp_path / "missing-load.json",
        tmp_path / "missing-dbt.json",
    )
    assert values["streaming_batch_metrics_available"] == 0
    assert values["streaming_bigquery_load_status_available"] == 0
    assert values["streaming_dbt_test_results_available"] == 0


def test_alertmanager_payload_writes_local_alert_without_slack(tmp_path):
    payload = {
        "status": "firing",
        "alerts": [
            {
                "status": "firing",
                "labels": {"alertname": "StreamingDlqEventsDetected", "severity": "WARNING"},
                "annotations": {"summary": "DLQ event detected"},
            }
        ],
    }
    output = tmp_path / "alerts.json"
    alerts = persist_and_emit(payload, output, None)
    assert alerts[0]["alert_name"] == "StreamingDlqEventsDetected"
    assert json.loads(output.read_text(encoding="utf-8"))[0]["severity"] == "WARNING"


def test_normalize_alerts_handles_empty_payload():
    assert normalize_alerts({}) == []
