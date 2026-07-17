"""Pure parsing, validation, and routing helpers for Beam transforms."""

import json
from datetime import datetime, timezone
from typing import Any

from streaming.contracts import parse_iso_timestamp, validate_event


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def decode_payload(payload: object) -> tuple[dict[str, Any] | None, str, list[str]]:
    """Decode a Pub/Sub payload without raising on malformed input."""
    if isinstance(payload, bytes):
        raw_payload = payload.decode("utf-8", errors="replace")
    elif isinstance(payload, str):
        raw_payload = payload
    elif isinstance(payload, dict):
        raw_payload = json.dumps(payload, sort_keys=True)
        return payload, raw_payload, []
    else:
        raw_payload = repr(payload)
        return None, raw_payload, ["event_not_json_object"]

    try:
        decoded = json.loads(raw_payload)
    except json.JSONDecodeError:
        return None, raw_payload, ["invalid_json"]
    if not isinstance(decoded, dict):
        return None, raw_payload, ["event_not_json_object"]
    return decoded, raw_payload, []


def event_errors(event: object) -> list[str]:
    errors = list(validate_event(event))
    if isinstance(event, dict):
        for field in (
            "transaction_id",
            "customer_id",
            "merchant_id",
            "currency",
            "country",
            "merchant_category",
            "payment_method",
            "device_id",
        ):
            if field in event and not isinstance(event[field], str):
                errors.append(f"invalid_{field}")
        amount = event.get("amount")
        if type(amount) in (int, float) and amount > 1_000_000:
            errors.append("invalid_amount")
        event_timestamp = parse_iso_timestamp(event.get("event_timestamp"))
        ingestion_timestamp = parse_iso_timestamp(event.get("ingestion_timestamp"))
        if event_timestamp and event_timestamp.tzinfo is None:
            errors.append("invalid_event_timestamp")
        if ingestion_timestamp and ingestion_timestamp.tzinfo is None:
            errors.append("invalid_ingestion_timestamp")
        if (
            event_timestamp
            and ingestion_timestamp
            and event_timestamp.tzinfo is not None
            and ingestion_timestamp.tzinfo is not None
            and event_timestamp > ingestion_timestamp
        ):
            errors.append("event_timestamp_after_ingestion_timestamp")
    return sorted(set(errors))


def error_field(errors: list[str]) -> str | None:
    if not errors:
        return None
    first = errors[0]
    for prefix in ("missing_", "invalid_"):
        if first.startswith(prefix):
            return first.removeprefix(prefix)
    if first.startswith("unexpected_fields:"):
        return first.split(":", 1)[1].split(",", 1)[0]
    if first == "event_timestamp_after_ingestion_timestamp":
        return "event_timestamp"
    return None


def build_valid_row(
    event: dict[str, Any], run_id: str, processing_time: datetime | None = None
) -> dict[str, Any]:
    processing_time = processing_time or utc_now()
    event_time = parse_iso_timestamp(event["event_timestamp"])
    if event_time is None:
        raise ValueError("event_timestamp must be validated before row construction")
    return {
        **event,
        "processing_timestamp": iso_utc(processing_time),
        "event_date": event_time.date().isoformat(),
        "event_hour": event_time.hour,
        "run_id": run_id,
        "source_system": "gcp_pubsub",
        "validation_status": "valid",
    }


def build_quarantine_row(
    raw_payload: str,
    errors: list[str],
    run_id: str,
    event: dict[str, Any] | None = None,
    processing_time: datetime | None = None,
) -> dict[str, Any]:
    processing_time = processing_time or utc_now()
    return {
        "raw_payload": raw_payload,
        "error_reason": ";".join(errors) or "unknown_validation_error",
        "error_field": error_field(errors),
        "ingestion_timestamp": event.get("ingestion_timestamp") if event else None,
        "processing_timestamp": iso_utc(processing_time),
        "run_id": run_id,
    }


def build_observation(
    run_id: str,
    metric_name: str,
    status: str,
    severity: str,
    details: str = "",
    processing_time: datetime | None = None,
) -> dict[str, Any]:
    return {
        "observation_timestamp": iso_utc(processing_time or utc_now()),
        "run_id": run_id,
        "metric_name": metric_name,
        "metric_value": 1.0,
        "status": status,
        "severity": severity,
        "details": details,
    }


def route_payload(
    payload: object, run_id: str, processing_time: datetime | None = None
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    """Return route, table row, and observation for one input payload."""
    event, raw_payload, decode_errors = decode_payload(payload)
    errors = decode_errors or event_errors(event)
    if errors:
        return (
            "invalid",
            build_quarantine_row(raw_payload, errors, run_id, event, processing_time),
            build_observation(
                run_id,
                "invalid_records",
                "WARN",
                "WARNING",
                ",".join(errors),
                processing_time,
            ),
        )
    assert event is not None
    return (
        "valid",
        build_valid_row(event, run_id, processing_time),
        build_observation(
            run_id, "valid_records", "PASS", "INFO", processing_time=processing_time
        ),
    )


def bigquery_failure_rows(
    failed: object, run_id: str, processing_time: datetime | None = None
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Convert a Beam BigQuery failed-row payload to quarantine and observation rows."""
    if isinstance(failed, tuple) and len(failed) >= 2:
        destination, row = failed[0], failed[1]
        details = f"destination={destination} row={row!r}"
    else:
        details = repr(failed)
    quarantine = build_quarantine_row(
        details,
        ["bigquery_write_failure"],
        run_id,
        processing_time=processing_time,
    )
    observation = build_observation(
        run_id,
        "bigquery_write_failures",
        "FAIL",
        "ERROR",
        details[:1000],
        processing_time,
    )
    return quarantine, observation
