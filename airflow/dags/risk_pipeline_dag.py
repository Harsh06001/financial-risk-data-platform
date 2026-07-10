import os
import shlex
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PYTHON_EXECUTABLE = PROJECT_ROOT / ".venv" / "bin" / "python"
DBT_EXECUTABLE = os.environ.get(
    "DBT_EXECUTABLE",
    str(PROJECT_ROOT / ".venv-dbt2" / "bin" / "dbt"),
)
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt" / "risk_analytics"


def shell_command(*arguments: object) -> str:
    return shlex.join(str(argument) for argument in arguments)


with DAG(
    dag_id="risk_pipeline_dag",
    start_date=datetime(2026, 6, 8),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
    },
) as dag:
    run_local_batch_pipeline = BashOperator(
        task_id="run_local_batch_pipeline",
        bash_command=shell_command(
            PYTHON_EXECUTABLE,
            "batch-processing/run_batch_pipeline.py",
            "--expected-event-dates",
            "31",
        ),
        cwd=str(PROJECT_ROOT),
    )

    sync_processed_to_gcs = BashOperator(
        task_id="sync_processed_to_gcs",
        bash_command=shell_command(
            PYTHON_EXECUTABLE,
            "cloud/sync_processed_to_gcs.py",
        ),
        cwd=str(PROJECT_ROOT),
    )

    sync_analytics_to_gcs = BashOperator(
        task_id="sync_analytics_to_gcs",
        bash_command=shell_command(
            PYTHON_EXECUTABLE,
            "cloud/sync_analytics_to_gcs.py",
        ),
        cwd=str(PROJECT_ROOT),
    )

    load_bigquery_tables = BashOperator(
        task_id="load_bigquery_tables",
        bash_command=shell_command(
            PYTHON_EXECUTABLE,
            "warehouse/load_bigquery_tables.py",
        ),
        cwd=str(PROJECT_ROOT),
    )

    validate_bigquery_tables = BashOperator(
        task_id="validate_bigquery_tables",
        bash_command=shell_command(
            PYTHON_EXECUTABLE,
            "warehouse/validate_bigquery_tables.py",
        ),
        cwd=str(PROJECT_ROOT),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=shell_command(
            DBT_EXECUTABLE,
            "run",
            "--project-dir",
            DBT_PROJECT_DIR,
            "--profiles-dir",
            DBT_PROJECT_DIR,
        ),
        cwd=str(PROJECT_ROOT),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=shell_command(
            DBT_EXECUTABLE,
            "test",
            "--project-dir",
            DBT_PROJECT_DIR,
            "--profiles-dir",
            DBT_PROJECT_DIR,
        ),
        cwd=str(PROJECT_ROOT),
    )

    (
        run_local_batch_pipeline
        >> sync_processed_to_gcs
        >> sync_analytics_to_gcs
        >> load_bigquery_tables
        >> validate_bigquery_tables
        >> dbt_run
        >> dbt_test
    )
