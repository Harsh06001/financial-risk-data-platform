"""Freshness checks with explicit UTC thresholds."""

from datetime import datetime, timezone

from observability.check_anomalies import check_range
from observability.models import Observation


def check_freshness(
    *,
    latest_timestamp: datetime,
    maximum_age_hours: float,
    pipeline_name: str,
    run_id: str,
    table_name: str,
    now: datetime | None = None,
) -> Observation:
    current = now or datetime.now(timezone.utc)
    if latest_timestamp.tzinfo is None:
        latest_timestamp = latest_timestamp.replace(tzinfo=timezone.utc)
    age_hours = max(0.0, (current - latest_timestamp).total_seconds() / 3600)
    return check_range(
        pipeline_name=pipeline_name,
        run_id=run_id,
        table_name=table_name,
        metric_name="freshness_age_hours",
        metric_value=round(age_hours, 3),
        expected_min=0,
        expected_max=maximum_age_hours,
        details=f"latest_timestamp={latest_timestamp.isoformat()}",
    )
