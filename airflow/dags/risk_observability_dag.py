"""Separate local/cloud-enabled monitoring DAG; leaves the batch DAG unchanged."""

import os
import shlex
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PYTHON_EXECUTABLE = Path(
    os.environ.get(
        "AIRFLOW_PROJECT_PYTHON",
        str(PROJECT_ROOT / ".venv-v12" / "bin" / "python"),
    )
)
RESULTS_ROOT = PROJECT_ROOT / "observability" / "results" / "airflow"
ALERTS_ROOT = PROJECT_ROOT / "alerts" / "results" / "airflow"


def shell_command(*arguments: object) -> str:
    return shlex.join(str(argument) for argument in arguments)


with DAG(
    dag_id="risk_observability_dag",
    description="Portfolio-mode batch, dbt, and streaming observations and alerts.",
    start_date=datetime(2026, 7, 16),
    schedule_interval="@hourly",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["portfolio", "observability"],
) as dag:
    collect_bigquery_observability = BashOperator(
        task_id="collect_bigquery_observability",
        bash_command=shell_command(
            PYTHON_EXECUTABLE,
            "observability/collect_metrics.py",
            "--component",
            "bigquery",
            "--output",
            RESULTS_ROOT / "bigquery.json",
        ),
        cwd=str(PROJECT_ROOT),
    )

    collect_dbt_observability = BashOperator(
        task_id="collect_dbt_observability",
        bash_command=shell_command(
            PYTHON_EXECUTABLE,
            "observability/collect_metrics.py",
            "--component",
            "dbt",
            "--output",
            RESULTS_ROOT / "dbt.json",
        ),
        cwd=str(PROJECT_ROOT),
    )

    collect_streaming_observability = BashOperator(
        task_id="collect_streaming_observability",
        bash_command=shell_command(
            PYTHON_EXECUTABLE,
            "observability/collect_metrics.py",
            "--component",
            "streaming",
            "--output",
            RESULTS_ROOT / "streaming.json",
        ),
        cwd=str(PROJECT_ROOT),
    )

    evaluate_alert_rules = BashOperator(
        task_id="evaluate_alert_rules",
        trigger_rule="all_done",
        bash_command=shell_command(
            PYTHON_EXECUTABLE,
            "alerts/evaluate_alerts.py",
            "--observations",
            RESULTS_ROOT / "bigquery.json",
            "--observations",
            RESULTS_ROOT / "dbt.json",
            "--observations",
            RESULTS_ROOT / "streaming.json",
            "--output",
            ALERTS_ROOT / "evaluated.json",
        ),
        cwd=str(PROJECT_ROOT),
    )

    emit_alerts = BashOperator(
        task_id="emit_alerts",
        bash_command=shell_command(
            PYTHON_EXECUTABLE,
            "alerts/alert_manager.py",
            "--alerts-input",
            ALERTS_ROOT / "evaluated.json",
            "--output",
            ALERTS_ROOT / "latest.json",
        ),
        cwd=str(PROJECT_ROOT),
    )

    [
        collect_bigquery_observability,
        collect_dbt_observability,
        collect_streaming_observability,
    ] >> evaluate_alert_rules >> emit_alerts
