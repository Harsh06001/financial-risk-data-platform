"""Evaluate observation records into alert records without side effects."""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


SEVERITY_RANK = {"INFO": 0, "WARNING": 1, "CRITICAL": 2}


def evaluate_observations(
    observations: Iterable[dict], minimum_severity: str = "WARNING"
) -> list[dict]:
    minimum = minimum_severity.upper()
    if minimum not in SEVERITY_RANK:
        raise ValueError(f"Unsupported minimum severity: {minimum_severity}")
    alerts = []
    for observation in observations:
        severity = str(observation.get("severity", "INFO")).upper()
        status = str(observation.get("status", "PASS")).upper()
        if severity not in SEVERITY_RANK:
            raise ValueError(f"Unsupported observation severity: {severity}")
        if status in {"WARN", "FAIL"} and SEVERITY_RANK[severity] >= SEVERITY_RANK[minimum]:
            alerts.append(
                {
                    "alert_timestamp": datetime.now(timezone.utc).isoformat(),
                    "severity": severity,
                    "status": status,
                    "pipeline_name": observation.get("pipeline_name", "unknown"),
                    "table_name": observation.get("table_name", "unknown"),
                    "metric_name": observation.get("metric_name", "unknown"),
                    "metric_value": observation.get("metric_value"),
                    "details": observation.get("details", ""),
                }
            )
    return alerts


def load_observations(paths: list[Path]) -> list[dict]:
    observations: list[dict] = []
    for path in paths:
        if path.exists():
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, list):
                raise ValueError(f"Expected a JSON list in {path}")
            observations.extend(payload)
    return observations


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate observability alert rules.")
    parser.add_argument("--observations", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument(
        "--minimum-severity",
        default=os.environ.get("ALERT_MIN_SEVERITY", "WARNING"),
    )
    args = parser.parse_args()
    alerts = evaluate_observations(
        load_observations(args.observations), args.minimum_severity
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(alerts, indent=2) + "\n", encoding="utf-8")
    print(f"ALERT RULES EVALUATED: alerts={len(alerts)} output={args.output}")


if __name__ == "__main__":
    main()
