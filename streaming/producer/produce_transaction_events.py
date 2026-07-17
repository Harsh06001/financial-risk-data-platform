"""Produce deterministic synthetic transaction events to Kafka or JSONL."""

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator

from streaming.contracts import validate_event


BASE_EVENT_TIME = datetime(2026, 7, 15, 12, 0, tzinfo=timezone.utc)
CURRENCIES = ("USD", "CAD")
COUNTRIES = ("US", "CA")
CATEGORIES = ("fuel", "grocery", "restaurant", "travel")
PAYMENT_METHODS = ("bank_transfer", "credit_card", "debit_card")
EVENT_TYPES = ("authorization", "purchase", "refund")


def generate_events(
    count: int,
    seed: int,
    invalid_every: int = 0,
    duplicate_every: int = 0,
    late_every: int = 0,
) -> Iterator[dict[str, object]]:
    """Yield a reproducible event sequence with controlled quality cases."""
    if count < 0:
        raise ValueError("count must be non-negative")
    for name, value in (
        ("invalid_every", invalid_every),
        ("duplicate_every", duplicate_every),
        ("late_every", late_every),
    ):
        if value < 0:
            raise ValueError(f"{name} must be non-negative")

    rng = random.Random(seed)
    transaction_ids: list[str] = []
    for index in range(count):
        transaction_id = str(
            uuid.uuid5(uuid.NAMESPACE_URL, f"stream:{seed}:{index}")
        )
        if duplicate_every and index and index % duplicate_every == 0:
            transaction_id = transaction_ids[-1]
        transaction_ids.append(transaction_id)

        ingestion_time = BASE_EVENT_TIME + timedelta(seconds=index)
        event_time = ingestion_time - timedelta(seconds=rng.randint(0, 120))
        if late_every and index and index % late_every == 0:
            event_time -= timedelta(days=2)

        event: dict[str, object] = {
            "transaction_id": transaction_id,
            "event_timestamp": event_time.isoformat(),
            "customer_id": f"CUST_{rng.randint(1, 500):06d}",
            "merchant_id": f"MERCH_{rng.randint(1, 100):06d}",
            "amount": round(rng.uniform(1.0, 2500.0), 2),
            "currency": rng.choice(CURRENCIES),
            "country": rng.choice(COUNTRIES),
            "merchant_category": rng.choice(CATEGORIES),
            "payment_method": rng.choice(PAYMENT_METHODS),
            "device_id": f"DEV_{rng.randint(1, 1000):06d}",
            "event_type": rng.choice(EVENT_TYPES),
            "ingestion_timestamp": ingestion_time.isoformat(),
        }
        if invalid_every and index and index % invalid_every == 0:
            event["amount"] = -1.0
        yield event


def write_jsonl(events: list[dict[str, object]], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as output_file:
        for event in events:
            output_file.write(json.dumps(event, sort_keys=True) + "\n")


def publish_events(
    events: list[dict[str, object]],
    bootstrap_servers: str,
    topic: str,
    events_per_second: float,
) -> None:
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise RuntimeError(
            "Kafka producer dependency is missing; install requirements-streaming.txt"
        ) from exc

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers.split(","),
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
    )
    delay = 1 / events_per_second if events_per_second > 0 else 0
    try:
        for event in events:
            producer.send(
                topic,
                key=str(event["transaction_id"]),
                value=event,
            )
            if delay:
                time.sleep(delay)
        producer.flush(timeout=30)
    finally:
        producer.close(timeout=30)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Produce deterministic transaction events."
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
    )
    parser.add_argument(
        "--topic",
        default=os.environ.get("KAFKA_TOPIC", "transaction-events"),
    )
    parser.add_argument(
        "--count", type=int, default=int(os.environ.get("STREAM_EVENT_COUNT", "100"))
    )
    parser.add_argument(
        "--rate", type=float, default=float(os.environ.get("STREAM_EVENT_RATE", "20"))
    )
    parser.add_argument(
        "--seed", type=int, default=int(os.environ.get("STREAM_RANDOM_SEED", "202612"))
    )
    parser.add_argument(
        "--invalid-every",
        type=int,
        default=int(os.environ.get("STREAM_INVALID_EVERY", "20")),
    )
    parser.add_argument(
        "--duplicate-every",
        type=int,
        default=int(os.environ.get("STREAM_DUPLICATE_EVERY", "25")),
    )
    parser.add_argument(
        "--late-every",
        type=int,
        default=int(os.environ.get("STREAM_LATE_EVERY", "30")),
    )
    parser.add_argument(
        "--dry-run-output",
        type=Path,
        help="Write JSONL instead of connecting to Kafka.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    events = list(
        generate_events(
            count=args.count,
            seed=args.seed,
            invalid_every=args.invalid_every,
            duplicate_every=args.duplicate_every,
            late_every=args.late_every,
        )
    )
    invalid_count = sum(bool(validate_event(event)) for event in events)
    duplicate_count = len(events) - len(
        {str(event["transaction_id"]) for event in events}
    )

    if args.dry_run_output:
        write_jsonl(events, args.dry_run_output)
        mode = f"jsonl:{args.dry_run_output}"
    else:
        publish_events(
            events,
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            events_per_second=args.rate,
        )
        mode = f"kafka:{args.bootstrap_servers}/{args.topic}"

    print(
        "STREAM PRODUCER COMPLETE "
        f"mode={mode} events={len(events)} invalid={invalid_count} "
        f"duplicates={duplicate_count} seed={args.seed}"
    )


if __name__ == "__main__":
    main()
