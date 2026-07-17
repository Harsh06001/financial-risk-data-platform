import pytest

from streaming.spark.validate_stream_output import validate_metrics


def test_streaming_metrics_reconcile_and_report_quality():
    summary = validate_metrics(
        [
            {
                "input_count": 10,
                "clean_count": 8,
                "invalid_count": 1,
                "duplicate_count": 1,
                "late_count": 2,
                "schema_drift_count": 0,
                "reconciliation": True,
            }
        ],
        max_invalid_rate=0.1,
    )
    assert summary["invalid_rate"] == 0.1
    assert summary["schema_drift_count"] == 0


def test_streaming_metrics_reject_false_reconciliation():
    with pytest.raises(RuntimeError, match="reconciliation"):
        validate_metrics(
            [
                {
                    "input_count": 2,
                    "clean_count": 2,
                    "invalid_count": 0,
                    "duplicate_count": 0,
                    "late_count": 0,
                    "reconciliation": False,
                }
            ],
            max_invalid_rate=0.1,
        )
