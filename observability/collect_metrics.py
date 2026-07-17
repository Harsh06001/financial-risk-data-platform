"""Collect local-first observations and optional live BigQuery observations."""

import argparse
import csv
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from observability.check_anomalies import check_range
from observability.models import Observation
from streaming.spark.validate_stream_output import load_metrics, validate_metrics


DEFAULT_OUTPUT = PROJECT_ROOT / "observability" / "results" / "latest.json"


def local_evidence_observations(run_id: str) -> list[Observation]:
    observations: list[Observation] = []
    scale_path = PROJECT_ROOT / "benchmarks" / "results" / "scale_test_results.csv"
    with scale_path.open(encoding="utf-8", newline="") as handle:
        scale_rows = list(csv.DictReader(handle))
    observations.append(
        check_range(
            pipeline_name="tracked_local_evidence",
            run_id=run_id,
            table_name="scale_test_results",
            metric_name="reconciled_scale_runs",
            metric_value=sum(row["status"] == "PASSED" for row in scale_rows),
            expected_min=len(scale_rows),
            expected_max=len(scale_rows),
            details="Reads committed evidence; it does not rerun benchmarks.",
        )
    )

    merge_path = (
        PROJECT_ROOT / "benchmarks" / "results" / "incremental_merge_evidence.csv"
    )
    with merge_path.open(encoding="utf-8", newline="") as handle:
        merge_rows = list(csv.DictReader(handle))
    observations.append(
        check_range(
            pipeline_name="tracked_local_evidence",
            run_id=run_id,
            table_name="incremental_merge_evidence",
            metric_name="duplicate_rows",
            metric_value=sum(int(row["duplicate_rows"]) for row in merge_rows),
            expected_min=0,
            expected_max=0,
            details="Reads committed isolated late-arrival evidence.",
        )
    )
    return observations


def streaming_observations(run_id: str, metrics_root: Path) -> list[Observation]:
    metrics = load_metrics(metrics_root)
    if not metrics:
        return [
            Observation(
                pipeline_name="transaction_event_stream",
                run_id=run_id,
                table_name="streaming_transaction_events",
                metric_name="batch_metrics_available",
                metric_value=0,
                expected_min=1,
                status="WARN",
                severity="WARNING",
                details=f"No generated batch metrics under {metrics_root}; run the demo first.",
            )
        ]
    summary = validate_metrics(metrics, max_invalid_rate=1.0)
    return [
        check_range(
            pipeline_name="transaction_event_stream",
            run_id=run_id,
            table_name="streaming_transaction_events",
            metric_name="clean_event_count",
            metric_value=int(summary["clean_count"]),
            expected_min=1,
        ),
        check_range(
            pipeline_name="transaction_event_stream",
            run_id=run_id,
            table_name="streaming_transaction_events",
            metric_name="invalid_event_rate",
            metric_value=float(summary["invalid_rate"]),
            expected_min=0,
            expected_max=0.10,
        ),
        check_range(
            pipeline_name="transaction_event_stream",
            run_id=run_id,
            table_name="streaming_transaction_events",
            metric_name="reconciled_batches",
            metric_value=sum(bool(row.get("reconciliation")) for row in metrics),
            expected_min=len(metrics),
            expected_max=len(metrics),
        ),
        check_range(
            pipeline_name="transaction_event_stream",
            run_id=run_id,
            table_name="streaming_transaction_events",
            metric_name="schema_drift_count",
            metric_value=int(summary["schema_drift_count"]),
            expected_min=0,
            expected_max=0,
        ),
    ]


def dbt_observations(run_id: str, run_results: Path) -> list[Observation]:
    if not run_results.exists():
        return [
            Observation(
                pipeline_name="dbt",
                run_id=run_id,
                table_name="dbt_run_results",
                metric_name="run_results_available",
                metric_value=0,
                expected_min=1,
                status="WARN",
                severity="WARNING",
                details=f"No dbt artifact at {run_results}; run dbt first.",
            )
        ]
    payload = json.loads(run_results.read_text(encoding="utf-8"))
    results = payload.get("results", [])
    failed = sum(row.get("status") in {"error", "fail"} for row in results)
    return [
        check_range(
            pipeline_name="dbt",
            run_id=run_id,
            table_name="dbt_run_results",
            metric_name="failed_nodes",
            metric_value=failed,
            expected_min=0,
            expected_max=0,
            details=f"nodes_observed={len(results)}",
        )
    ]


def bigquery_observations(run_id: str) -> list[Observation]:
    project = os.environ.get("GCP_PROJECT_ID", "risk-data-platform-npg-2026")
    dataset = os.environ.get("DBT_DATASET", "risk_analytics")
    location = os.environ.get("GCP_LOCATION", "us-central1")
    query = f"""
    SELECT COUNT(*) AS row_count,
           COUNT(DISTINCT transaction_id) AS unique_ids,
           COUNTIF(transaction_id IS NULL) AS null_ids,
           COUNT(DISTINCT event_date) AS partition_dates,
           (SELECT COUNT(*)
            FROM `{project}.{dataset}.INFORMATION_SCHEMA.PARTITIONS`
            WHERE table_name = 'processed_transactions') AS physical_partitions
    FROM `{project}.{dataset}.processed_transactions`
    """
    completed = subprocess.run(
        [
            "bq",
            f"--project_id={project}",
            f"--location={location}",
            "query",
            "--format=json",
            "--use_legacy_sql=false",
            query,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    row = json.loads(completed.stdout)[0]
    count = int(row["row_count"])
    return [
        check_range(
            pipeline_name="bigquery_warehouse",
            run_id=run_id,
            table_name="processed_transactions",
            metric_name="row_count",
            metric_value=count,
            expected_min=100350,
            expected_max=100350,
        ),
        check_range(
            pipeline_name="bigquery_warehouse",
            run_id=run_id,
            table_name="processed_transactions",
            metric_name="duplicate_transaction_ids",
            metric_value=count - int(row["unique_ids"]),
            expected_min=0,
            expected_max=0,
        ),
        check_range(
            pipeline_name="bigquery_warehouse",
            run_id=run_id,
            table_name="processed_transactions",
            metric_name="null_transaction_ids",
            metric_value=int(row["null_ids"]),
            expected_min=0,
            expected_max=0,
        ),
        check_range(
            pipeline_name="bigquery_warehouse",
            run_id=run_id,
            table_name="processed_transactions",
            metric_name="event_date_count",
            metric_value=int(row["partition_dates"]),
            expected_min=31,
            expected_max=31,
        ),
        check_range(
            pipeline_name="bigquery_warehouse",
            run_id=run_id,
            table_name="processed_transactions",
            metric_name="physical_partition_count",
            metric_value=int(row["physical_partitions"]),
            expected_min=31,
            expected_max=31,
        ),
    ]


def write_observations(observations: list[Observation], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps([item.to_dict() for item in observations], indent=2) + "\n",
        encoding="utf-8",
    )
    for item in observations:
        print(
            f"[{item.status}/{item.severity}] {item.pipeline_name}."
            f"{item.table_name}.{item.metric_name}={item.metric_value}"
        )
    print(f"OBSERVATIONS WRITTEN: {output} ({len(observations)} checks)")


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect lightweight data observations.")
    parser.add_argument(
        "--component",
        choices=["all", "local", "streaming", "dbt", "bigquery"],
        default="all",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--streaming-metrics-root",
        type=Path,
        default=PROJECT_ROOT / "data/streaming/metrics/transaction_events",
    )
    parser.add_argument(
        "--dbt-run-results",
        type=Path,
        default=PROJECT_ROOT / "dbt/risk_analytics/target/run_results.json",
    )
    parser.add_argument("--run-id", default=str(uuid.uuid4()))
    args = parser.parse_args()

    observations: list[Observation] = []
    collection_error: Exception | None = None
    components = (
        {"local", "streaming", "dbt"}
        if args.component == "all"
        else {args.component}
    )
    if "local" in components:
        observations.extend(local_evidence_observations(args.run_id))
    if "streaming" in components:
        observations.extend(streaming_observations(args.run_id, args.streaming_metrics_root))
    if "dbt" in components:
        observations.extend(dbt_observations(args.run_id, args.dbt_run_results))
    if "bigquery" in components:
        try:
            observations.extend(bigquery_observations(args.run_id))
        except Exception as exc:  # preserve an alertable record before failing the task
            collection_error = exc
            observations.append(
                Observation(
                    pipeline_name="bigquery_warehouse",
                    run_id=args.run_id,
                    table_name="processed_transactions",
                    metric_name="collection_success",
                    metric_value=0,
                    expected_min=1,
                    expected_max=1,
                    status="FAIL",
                    severity="CRITICAL",
                    details=f"{type(exc).__name__}: {exc}",
                )
            )
    write_observations(observations, args.output)
    if collection_error is not None:
        raise RuntimeError("BigQuery observation collection failed") from collection_error


if __name__ == "__main__":
    main()
