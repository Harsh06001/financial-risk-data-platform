import os
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "risk_analytics"


def resolve_dbt_executable() -> str:
    return os.environ.get(
        "DBT_EXECUTABLE",
        str(PROJECT_ROOT / ".venv-dbt2" / "bin" / "dbt"),
    )


def run_stage(label: str, command: list[str]) -> None:
    print(f"\n[{label}]")
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def main() -> None:
    dbt_executable = resolve_dbt_executable()
    stages = [
        ("1/7 Running local batch pipeline", [sys.executable, "batch-processing/run_batch_pipeline.py", "--expected-event-dates", "31"]),
        ("2/7 Syncing processed data", [sys.executable, "cloud/sync_processed_to_gcs.py"]),
        ("3/7 Syncing analytics data", [sys.executable, "cloud/sync_analytics_to_gcs.py"]),
        ("4/7 Loading BigQuery warehouse", [sys.executable, "warehouse/load_bigquery_tables.py"]),
        ("5/7 Validating BigQuery warehouse", [sys.executable, "warehouse/validate_bigquery_tables.py"]),
        (
            "6/7 Running dbt models",
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
            "7/7 Running dbt tests",
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

    print("\nEND-TO-END CLOUD PIPELINE COMPLETE")


if __name__ == "__main__":
    main()
