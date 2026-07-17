import json
from datetime import datetime, timezone

from gcp_streaming.beam_pipeline.validation import (
    bigquery_failure_rows,
    route_payload,
)
from streaming.producer.produce_transaction_events import generate_events


NOW = datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc)


def test_valid_payload_routes_to_explicit_bigquery_row():
    event = next(generate_events(1, seed=4))
    route, row, observation = route_payload(json.dumps(event).encode(), "run-1", NOW)
    assert route == "valid"
    assert row["transaction_id"] == event["transaction_id"]
    assert row["event_date"] == str(event["event_timestamp"])[:10]
    assert row["run_id"] == "run-1"
    assert row["source_system"] == "gcp_pubsub"
    assert row["validation_status"] == "valid"
    assert observation["metric_name"] == "valid_records"


def test_invalid_payload_routes_to_quarantine_with_reason():
    event = next(generate_events(1, seed=4))
    event["amount"] = -2
    route, row, observation = route_payload(event, "run-2", NOW)
    assert route == "invalid"
    assert row["error_reason"] == "invalid_amount"
    assert row["error_field"] == "amount"
    assert row["run_id"] == "run-2"
    assert observation["severity"] == "WARNING"


def test_malformed_json_is_quarantined_without_crashing():
    route, row, _ = route_payload(b"{not-json", "run-3", NOW)
    assert route == "invalid"
    assert row["raw_payload"] == "{not-json"
    assert row["error_reason"] == "invalid_json"


def test_impossible_timestamp_is_rejected():
    event = next(generate_events(1, seed=4))
    event["event_timestamp"] = "2026-07-15T13:00:00+00:00"
    event["ingestion_timestamp"] = "2026-07-15T12:00:00+00:00"
    route, row, _ = route_payload(event, "run-4", NOW)
    assert route == "invalid"
    assert row["error_field"] == "event_timestamp"


def test_out_of_range_amount_and_wrong_field_type_are_rejected():
    event = next(generate_events(1, seed=4))
    event["amount"] = 1_000_001
    event["customer_id"] = 42
    route, row, _ = route_payload(event, "run-4b", NOW)
    assert route == "invalid"
    assert "invalid_amount" in row["error_reason"]
    assert "invalid_customer_id" in row["error_reason"]


def test_bigquery_failure_constructs_quarantine_and_observation_rows():
    quarantine, observation = bigquery_failure_rows(
        ("project:dataset.table", {"transaction_id": "t-1"}, [{"reason": "invalid"}]),
        "run-5",
        NOW,
    )
    assert quarantine["error_reason"] == "bigquery_write_failure"
    assert observation["metric_name"] == "bigquery_write_failures"
    assert observation["status"] == "FAIL"
