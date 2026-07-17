"""Snapshot and verify that a bounded restart did not replay completed batches."""

import argparse
import json
from pathlib import Path

from streaming.spark.validate_stream_output import load_metrics, validate_metrics


def metric_fingerprint(metrics_root: Path) -> dict[str, object]:
    metrics = load_metrics(metrics_root)
    summary = validate_metrics(metrics, max_invalid_rate=1.0)
    return {
        "metric_files": sorted(path.name for path in metrics_root.glob("batch_id=*.json")),
        "batch_count": summary["batch_count"],
        "input_count": summary["input_count"],
        "clean_count": summary["clean_count"],
        "invalid_count": summary["invalid_count"],
        "duplicate_count": summary["duplicate_count"],
        "dlq_count": summary["dlq_count"],
        "risk_alert_count": summary["risk_alert_count"],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify checkpoint restart behavior.")
    parser.add_argument("--metrics-root", type=Path, required=True)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--write-snapshot", action="store_true")
    args = parser.parse_args()

    current = metric_fingerprint(args.metrics_root)
    if args.write_snapshot:
        args.snapshot.parent.mkdir(parents=True, exist_ok=True)
        args.snapshot.write_text(json.dumps(current, indent=2) + "\n", encoding="utf-8")
        print(f"CHECKPOINT SNAPSHOT WRITTEN: {args.snapshot}")
        return

    expected = json.loads(args.snapshot.read_text(encoding="utf-8"))
    if current != expected:
        raise RuntimeError(
            "Checkpoint restart changed processed batch metrics; offsets may have replayed. "
            f"expected={expected} actual={current}"
        )
    print(f"CHECKPOINT RESTART VALIDATION PASSED: {current}")


if __name__ == "__main__":
    main()
