"""Pure anomaly checks used by collectors and tests."""

from typing import Optional

from observability.models import Observation


def check_range(
    *,
    pipeline_name: str,
    run_id: str,
    table_name: str,
    metric_name: str,
    metric_value: float,
    expected_min: Optional[float] = None,
    expected_max: Optional[float] = None,
    warn_only: bool = False,
    details: str = "",
) -> Observation:
    below = expected_min is not None and metric_value < expected_min
    above = expected_max is not None and metric_value > expected_max
    outside = below or above
    return Observation(
        pipeline_name=pipeline_name,
        run_id=run_id,
        table_name=table_name,
        metric_name=metric_name,
        metric_value=metric_value,
        expected_min=expected_min,
        expected_max=expected_max,
        status=("WARN" if warn_only else "FAIL") if outside else "PASS",
        severity=("WARNING" if warn_only else "CRITICAL") if outside else "INFO",
        details=details,
    )
