"""Shared streaming event contract and pure validation helpers."""

from datetime import datetime
from typing import Any


EVENT_FIELDS = (
    "transaction_id",
    "event_timestamp",
    "customer_id",
    "merchant_id",
    "amount",
    "currency",
    "country",
    "merchant_category",
    "payment_method",
    "device_id",
    "event_type",
    "ingestion_timestamp",
)
VALID_EVENT_TYPES = {"authorization", "purchase", "refund"}


def parse_iso_timestamp(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def validate_event(event: object) -> list[str]:
    """Return contract violations; an empty list means the event is valid."""
    if not isinstance(event, dict):
        return ["event_not_object"]

    errors: list[str] = []
    unexpected = sorted(set(event) - set(EVENT_FIELDS))
    if unexpected:
        errors.append(f"unexpected_fields:{','.join(unexpected)}")
    for field in EVENT_FIELDS:
        if field not in event or event[field] is None or event[field] == "":
            errors.append(f"missing_{field}")

    amount = event.get("amount")
    if type(amount) not in (int, float) or amount <= 0:
        errors.append("invalid_amount")
    if event.get("event_type") not in VALID_EVENT_TYPES:
        errors.append("invalid_event_type")
    if parse_iso_timestamp(event.get("event_timestamp")) is None:
        errors.append("invalid_event_timestamp")
    if parse_iso_timestamp(event.get("ingestion_timestamp")) is None:
        errors.append("invalid_ingestion_timestamp")
    return sorted(set(errors))
