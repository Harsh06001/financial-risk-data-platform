import argparse
import os
import subprocess
import sys
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "risk_analytics"
PROCESSED_ROOT = PROJECT_ROOT / "data" / "processed" / "transactions"


def parse_event_date(value: str) -> str:
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "event date must use YYYY-MM-DD format"
        ) from exc


def run_stage(label: str, command: list[str]) -> None:
    print(f"\n[{label}]")
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process and upsert one financial transaction event date."
    )
    parser.add_argument("--event-date", required=True, type=parse_event_date)
    parser.add_argument(
        "--raw-input",
        default="data/raw/transactions/*.csv",
        help="Raw CSV path or glob containing the selected event date.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    dbt_executable = os.environ.get(
        "DBT_EXECUTABLE",
        str(PROJECT_ROOT / ".venv-dbt2" / "bin" / "dbt"),
    )
    stages = [
        (
            "1/6 Process selected event_date",
            [
                sys.executable,
                "batch-processing/process_transactions.py",
                "--input",
                args.raw_input,
                "--output",
                str(PROCESSED_ROOT),
                "--write-strategy",
                "repartitioned",
                "--event-date",
                args.event_date,
            ],
        ),
        (
            "2/6 Validate selected local partition",
            [
                sys.executable,
                "batch-processing/validate_processed_transactions.py",
                "--input",
                str(PROCESSED_ROOT),
                "--expected-event-dates",
                "1",
                "--event-date",
                args.event_date,
            ],
        ),
        (
            "3/6 Mirror selected partition to GCS",
            [
                sys.executable,
                "cloud/sync_processed_partition_to_gcs.py",
                "--event-date",
                args.event_date,
            ],
        ),
        (
            "4/6 MERGE selected partition into BigQuery",
            [
                sys.executable,
                "warehouse/merge_processed_partition.py",
                "--event-date",
                args.event_date,
            ],
        ),
        (
            "5/6 Run downstream dbt models",
            [
                dbt_executable,
                "run",
                "--project-dir",
                str(DBT_PROJECT_DIR),
                "--profiles-dir",
                str(DBT_PROJECT_DIR),
            ],
        ),
        (
            "6/6 Test downstream dbt models",
            [
                dbt_executable,
                "test",
                "--project-dir",
                str(DBT_PROJECT_DIR),
                "--profiles-dir",
                str(DBT_PROJECT_DIR),
            ],
        ),
    ]

    for label, command in stages:
        run_stage(label, command)

    print(
        f"\nINCREMENTAL PIPELINE COMPLETE: event_date={args.event_date}"
    )


if __name__ == "__main__":
    main()
