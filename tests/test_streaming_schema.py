from datetime import datetime

import pytest

from streaming.contracts import validate_event
from streaming.producer.produce_transaction_events import generate_events


def test_event_generation_is_deterministic():
    first = list(generate_events(12, seed=42, invalid_every=5, duplicate_every=7))
    second = list(generate_events(12, seed=42, invalid_every=5, duplicate_every=7))
    assert first == second


def test_controlled_quality_cases_are_injected():
    events = list(
        generate_events(
            11,
            seed=7,
            invalid_every=5,
            duplicate_every=4,
            late_every=3,
        )
    )
    assert sum(bool(validate_event(event)) for event in events) == 2
    assert len(events) - len({event["transaction_id"] for event in events}) == 2
    late = events[3]
    event_time = datetime.fromisoformat(str(late["event_timestamp"]))
    ingestion_time = datetime.fromisoformat(str(late["ingestion_timestamp"]))
    assert (ingestion_time - event_time).total_seconds() > 24 * 60 * 60


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("transaction_id", ""),
        ("amount", 0),
        ("event_type", "unknown"),
        ("event_timestamp", "not-a-timestamp"),
    ],
)
def test_event_contract_rejects_invalid_fields(field, value):
    event = next(generate_events(1, seed=99))
    event[field] = value
    assert validate_event(event)


def test_event_contract_rejects_unexpected_fields_as_schema_drift():
    event = next(generate_events(1, seed=99))
    event["new_uncontracted_field"] = "drift"
    assert validate_event(event) == ["unexpected_fields:new_uncontracted_field"]
