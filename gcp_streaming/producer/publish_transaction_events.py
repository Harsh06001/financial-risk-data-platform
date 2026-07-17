"""Publish deterministic transaction events to GCP Pub/Sub with hard guardrails."""

import argparse
import json
import os
import time
from pathlib import Path

from streaming.producer.produce_transaction_events import generate_events


DEFAULT_COUNT = 1000
MAX_UNAPPROVED_COUNT = 10_000


def validate_publish_request(
    count: int, allow_large_demo: bool, dry_run: bool, acknowledged: bool
) -> None:
    if count < 0:
        raise ValueError("count must be non-negative")
    if count > MAX_UNAPPROVED_COUNT and not allow_large_demo:
        raise ValueError(
            f"refusing {count} events; counts above {MAX_UNAPPROVED_COUNT} require "
            "--allow-large-demo"
        )
    if not dry_run and not acknowledged:
        raise ValueError(
            "live Pub/Sub publishing requires --acknowledge-cost-risk or "
            "ACKNOWLEDGE_GCP_COST_RISK=true"
        )


def publish_events(
    events: list[dict[str, object]],
    project_id: str,
    topic: str,
    events_per_second: float,
) -> None:
    try:
        from google.cloud import pubsub_v1
    except ImportError as exc:
        raise RuntimeError(
            "Pub/Sub dependency is missing; install requirements-gcp-streaming.txt"
        ) from exc

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic)
    delay = 1 / events_per_second if events_per_second > 0 else 0
    futures = []
    for event in events:
        payload = json.dumps(event, sort_keys=True).encode("utf-8")
        futures.append(
            publisher.publish(
                topic_path,
                payload,
                transaction_id=str(event["transaction_id"]),
                source_system="gcp_streaming_demo",
            )
        )
        if delay:
            time.sleep(delay)
    for future in futures:
        future.result(timeout=60)


def write_dry_run(events: list[dict[str, object]], output: Path | None) -> None:
    lines = [json.dumps(event, sort_keys=True) for event in events]
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    else:
        for line in lines:
            print(line)


def parse_arguments(arguments: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-id", default=os.environ.get("GCP_PROJECT_ID", ""))
    parser.add_argument(
        "--topic",
        default=os.environ.get("GCP_STREAMING_TOPIC", "transaction-events"),
    )
    parser.add_argument(
        "--count",
        type=int,
        default=int(os.environ.get("GCP_STREAMING_DEMO_EVENT_COUNT", DEFAULT_COUNT)),
    )
    parser.add_argument(
        "--rate",
        type=float,
        default=float(os.environ.get("GCP_STREAMING_EVENT_RATE", "5")),
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=int(os.environ.get("GCP_STREAMING_RANDOM_SEED", "202614")),
    )
    parser.add_argument("--invalid-every", type=int, default=20)
    parser.add_argument("--duplicate-every", type=int, default=0)
    parser.add_argument("--late-every", type=int, default=30)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--allow-large-demo", action="store_true")
    parser.add_argument("--acknowledge-cost-risk", action="store_true")
    return parser.parse_args(arguments)


def run(args: argparse.Namespace) -> list[dict[str, object]]:
    acknowledged = args.acknowledge_cost_risk or os.environ.get(
        "ACKNOWLEDGE_GCP_COST_RISK", ""
    ).lower() in {"1", "true", "yes"}
    validate_publish_request(
        args.count, args.allow_large_demo, args.dry_run, acknowledged
    )
    if not args.dry_run and not args.project_id:
        raise ValueError("--project-id or GCP_PROJECT_ID is required for live publishing")
    print(
        "GCP PUBSUB PRODUCER PREFLIGHT "
        f"mode={'DRY_RUN' if args.dry_run else 'LIVE_GCP'} events={args.count} "
        f"rate={args.rate}/s topic={args.topic}"
    )
    if not args.dry_run:
        print("WARNING: live Pub/Sub usage can create GCP charges.")

    events = list(
        generate_events(
            args.count,
            args.seed,
            args.invalid_every,
            args.duplicate_every,
            args.late_every,
        )
    )
    if args.dry_run:
        write_dry_run(events, args.output)
    else:
        publish_events(events, args.project_id, args.topic, args.rate)
    print(
        "GCP PUBSUB PRODUCER COMPLETE "
        f"mode={'dry-run' if args.dry_run else 'live'} events={len(events)} seed={args.seed}"
    )
    return events


def main() -> None:
    try:
        run(parse_arguments())
    except ValueError as exc:
        raise SystemExit(f"ERROR: {exc}") from exc


if __name__ == "__main__":
    main()
