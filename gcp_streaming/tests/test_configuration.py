from pathlib import Path

from gcp_streaming.beam_pipeline.schemas import (
    OBSERVATION_FIELDS,
    QUARANTINE_FIELDS,
    VALID_EVENT_FIELDS,
)
from gcp_streaming.beam_pipeline.streaming_to_bigquery import (
    table_spec,
    validate_worker_limits,
)


ROOT = Path(__file__).resolve().parents[2]


def test_explicit_schemas_have_required_routing_fields():
    valid = {field["name"] for field in VALID_EVENT_FIELDS}
    quarantine = {field["name"] for field in QUARANTINE_FIELDS}
    observations = {field["name"] for field in OBSERVATION_FIELDS}
    assert {"transaction_id", "processing_timestamp", "run_id", "validation_status"} <= valid
    assert {"raw_payload", "error_reason", "error_field", "run_id"} <= quarantine
    assert {"metric_name", "metric_value", "status", "severity"} <= observations


def test_beam_helpers_enforce_worker_limits():
    assert table_spec("p", "d", "t") == "p:d.t"
    validate_worker_limits(1, 1)
    try:
        validate_worker_limits(2, 2)
    except ValueError as error:
        assert "one initial worker" in str(error)
    else:
        raise AssertionError("unsafe worker count was accepted")


def test_terraform_defaults_are_opt_in_and_cost_bounded():
    variables = (ROOT / "infrastructure/terraform/variables_streaming.tf").read_text(encoding="utf-8")
    assert variables.count("default     = false") >= 3
    assert 'default     = 1000' in variables
    assert 'default     = 15' in variables
    assert variables.count('default     = 1') >= 2
    assert "billing_account_id" in variables
    assert "sensitive   = true" in variables


def test_cleanup_scripts_are_narrow_and_default_read_only():
    cleanup = (ROOT / "gcp_streaming/scripts/cleanup_streaming_demo.sh").read_text(encoding="utf-8")
    stop = (ROOT / "gcp_streaming/scripts/stop_dataflow_jobs.sh").read_text(encoding="utf-8")
    assert 'EXECUTE=false' in cleanup
    assert 'financial-risk-v14-demo-' in stop
    assert 'transaction-events-dataflow-sub' in cleanup
    assert 'v14-demo' in cleanup
    assert 'gs://*-streaming-dataflow-temp/v14-demo)' in cleanup


def test_gcp_examples_contain_no_embedded_credentials():
    content = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (ROOT / "gcp_streaming").rglob("*")
        if path.is_file() and "__pycache__" not in path.parts
    )
    assert "BEGIN " + "PRIVATE KEY" not in content
    assert "client_" + 'email":' not in content
    assert "private_" + 'key":' not in content


def test_manual_workflow_never_runs_on_push_or_applies_terraform():
    workflow = (ROOT / ".github/workflows/gcp-streaming-integration.yml").read_text(
        encoding="utf-8"
    )
    assert "workflow_dispatch:" in workflow
    assert "\n  push:" not in workflow
    assert "\n  pull_request:" not in workflow
    assert "terraform -chdir=infrastructure/terraform plan" in workflow
    assert "terraform apply" not in workflow
    assert "Always stop matching demo jobs" in workflow


def test_beam_definition_has_pubsub_split_quarantine_and_observations():
    pipeline = (
        ROOT / "gcp_streaming/beam_pipeline/streaming_to_bigquery.py"
    ).read_text(encoding="utf-8")
    assert "ReadFromPubSub" in pipeline
    assert "WriteValidEvents" in pipeline
    assert "RouteBigQueryWriteFailures" in pipeline
    assert "WriteQuarantine" in pipeline
    assert "WriteObservations" in pipeline
    assert "extended_error_info=True" in pipeline


def test_monitoring_definitions_are_optional_and_cover_required_signals():
    monitoring = (ROOT / "infrastructure/terraform/monitoring.tf").read_text(
        encoding="utf-8"
    )
    assert monitoring.count("var.enable_monitoring_alerts ? 1 : 0") == 6
    assert "subscription/num_undelivered_messages" in monitoring
    assert "job/is_failed" in monitoring
    assert "valid_records" in monitoring
    assert "invalid_records" in monitoring
    assert "bigquery_write_failures" in monitoring
    assert "job/elapsed_time" in monitoring
