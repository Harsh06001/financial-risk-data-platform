from datetime import datetime, timedelta, timezone

import pytest

from observability.check_anomalies import check_range
from observability.check_freshness import check_freshness
from observability.models import Observation


def test_range_check_passes_inside_contract():
    result = check_range(
        pipeline_name="test",
        run_id="run",
        table_name="table",
        metric_name="rows",
        metric_value=10,
        expected_min=10,
        expected_max=10,
    )
    assert (result.status, result.severity) == ("PASS", "INFO")


def test_range_check_can_warn_without_weakening_failure_default():
    common = dict(
        pipeline_name="test",
        run_id="run",
        table_name="table",
        metric_name="invalid_rate",
        metric_value=0.2,
        expected_max=0.05,
    )
    assert check_range(**common).status == "FAIL"
    assert check_range(**common, warn_only=True).status == "WARN"


def test_freshness_fails_after_sla():
    now = datetime(2026, 7, 16, tzinfo=timezone.utc)
    result = check_freshness(
        latest_timestamp=now - timedelta(hours=25),
        maximum_age_hours=24,
        pipeline_name="test",
        run_id="run",
        table_name="table",
        now=now,
    )
    assert result.status == "FAIL"


def test_observation_rejects_unknown_status():
    with pytest.raises(ValueError):
        Observation("p", "r", "t", "m", 1, status="UNKNOWN")
