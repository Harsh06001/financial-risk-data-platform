import ast
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_monitoring_dag_contract():
    path = PROJECT_ROOT / "airflow/dags/risk_observability_dag.py"
    source = path.read_text(encoding="utf-8")
    tree = ast.parse(source)
    task_ids = {
        keyword.value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        for keyword in node.keywords
        if keyword.arg == "task_id" and isinstance(keyword.value, ast.Constant)
    }
    assert task_ids == {
        "collect_bigquery_observability",
        "collect_dbt_observability",
        "collect_streaming_observability",
        "evaluate_alert_rules",
        "emit_alerts",
    }
    assert 'dag_id="risk_observability_dag"' in source
    assert 'schedule_interval="@hourly"' in source
    assert "catchup=False" in source
    assert 'trigger_rule="all_done"' in source
    assert "] >> evaluate_alert_rules >> emit_alerts" in source


def test_existing_batch_dag_keeps_seven_tasks():
    source = (PROJECT_ROOT / "airflow/dags/risk_pipeline_dag.py").read_text(
        encoding="utf-8"
    )
    tree = ast.parse(source)
    task_ids = [
        keyword.value.value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        for keyword in node.keywords
        if keyword.arg == "task_id" and isinstance(keyword.value, ast.Constant)
    ]
    assert len(task_ids) == 7
