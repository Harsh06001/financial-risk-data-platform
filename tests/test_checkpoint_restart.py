import json

from streaming.spark.verify_checkpoint_restart import metric_fingerprint


def test_checkpoint_fingerprint_is_stable_for_unchanged_metrics(tmp_path):
    metrics_root = tmp_path / "metrics"
    metrics_root.mkdir()
    payload = {
        "input_count": 4,
        "clean_count": 2,
        "invalid_count": 1,
        "duplicate_count": 1,
        "late_count": 0,
        "schema_drift_count": 0,
        "dlq_count": 1,
        "risk_alert_count": 1,
        "reconciliation": True,
    }
    (metrics_root / "batch_id=0.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )
    first = metric_fingerprint(metrics_root)
    second = metric_fingerprint(metrics_root)
    assert first == second
    assert first["input_count"] == 4
