"""Validate streaming micro-batch metadata without requiring cloud access."""

import argparse
import json
from pathlib import Path


DEFAULT_METRICS = Path("data/streaming/metrics/transaction_events")


def load_metrics(metrics_root: Path) -> list[dict[str, object]]:
    return [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted(metrics_root.glob("batch_id=*.json"))
    ]


def validate_metrics(
    metrics: list[dict[str, object]],
    max_invalid_rate: float,
) -> dict[str, object]:
    if not 0 <= max_invalid_rate <= 1:
        raise ValueError("max_invalid_rate must be between 0 and 1")
    if not metrics:
        raise RuntimeError("No streaming batch metrics were found")
    totals = {
        key: sum(int(batch[key]) for batch in metrics)
        for key in (
            "input_count",
            "clean_count",
            "invalid_count",
            "duplicate_count",
            "late_count",
        )
    }
    totals["schema_drift_count"] = sum(
        int(batch.get("schema_drift_count", 0)) for batch in metrics
    )
    totals["dlq_count"] = sum(int(batch.get("dlq_count", 0)) for batch in metrics)
    totals["risk_alert_count"] = sum(
        int(batch.get("risk_alert_count", 0)) for batch in metrics
    )
    if totals["dlq_count"] > totals["invalid_count"]:
        raise RuntimeError("DLQ count exceeds invalid event count")
    if not all(bool(batch.get("reconciliation")) for batch in metrics):
        raise RuntimeError("At least one streaming batch failed reconciliation")
    if totals["input_count"] != (
        totals["clean_count"]
        + totals["invalid_count"]
        + totals["duplicate_count"]
    ):
        raise RuntimeError("Aggregate streaming counts do not reconcile")
    invalid_rate = (
        totals["invalid_count"] / totals["input_count"]
        if totals["input_count"]
        else 0.0
    )
    if invalid_rate > max_invalid_rate:
        raise RuntimeError(
            f"Invalid rate {invalid_rate:.4f} exceeds {max_invalid_rate:.4f}"
        )
    return {
        **totals,
        "batch_count": len(metrics),
        "invalid_rate": invalid_rate,
        "status": "PASS",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate streaming metrics.")
    parser.add_argument("--metrics-root", type=Path, default=DEFAULT_METRICS)
    parser.add_argument("--max-invalid-rate", type=float, default=0.10)
    args = parser.parse_args()
    summary = validate_metrics(load_metrics(args.metrics_root), args.max_invalid_rate)
    print(f"STREAM OUTPUT VALIDATION PASSED: {summary}")


if __name__ == "__main__":
    main()
