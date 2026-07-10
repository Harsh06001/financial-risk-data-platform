import argparse
from datetime import date
from pathlib import Path

from sync_utils import (
    list_gcs_parquet_inventory,
    list_local_parquet_inventory,
    run_command,
    validate_exact_local_source,
    validate_gcs_destination,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESSED_BUCKET = "gs://risk-data-platform-npg-2026-processed-data"
SCOPES = {
    "canonical": (
        PROJECT_ROOT / "data" / "processed" / "transactions",
        f"{PROCESSED_BUCKET}/transactions",
    ),
    "demo": (
        PROJECT_ROOT / "data" / "incremental-demo" / "processed" / "transactions",
        f"{PROCESSED_BUCKET}/incremental-demo/transactions",
    ),
}


def parse_event_date(value: str) -> str:
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "event date must use YYYY-MM-DD format"
        ) from exc


def sync_partition(event_date: str, scope: str) -> None:
    local_root, destination_root = SCOPES[scope]
    local_partition = local_root / f"event_date={event_date}"
    local_partition = validate_exact_local_source(
        local_partition,
        local_partition,
        f"{scope} processed partition",
    )
    destination_uri = validate_gcs_destination(
        f"{destination_root}/event_date={event_date}",
        f"{destination_root}/event_date={event_date}",
        f"{scope} processed partition",
    )
    local_inventory = list_local_parquet_inventory(local_partition)

    if len(local_inventory) != 1:
        raise RuntimeError(
            f"Expected exactly one local Parquet file for {event_date}; "
            f"found {len(local_inventory)}. Refusing partition mirror sync."
        )

    root_inventory_before = list_gcs_parquet_inventory(
        destination_root,
        allow_missing=True,
    )
    target_prefix = f"event_date={event_date}/"
    unrelated_before = {
        item for item in root_inventory_before if not item.startswith(target_prefix)
    }

    print(
        f"Partition preflight passed: scope={scope}, event_date={event_date}, "
        f"local parquet files={len(local_inventory)}"
    )
    run_command(
        [
            "gcloud",
            "storage",
            "rsync",
            "--recursive",
            "--delete-unmatched-destination-objects",
            str(local_partition),
            destination_uri,
        ],
        cwd=PROJECT_ROOT,
    )

    remote_inventory = list_gcs_parquet_inventory(destination_uri)
    if remote_inventory != local_inventory:
        raise RuntimeError(
            "Incremental partition inventory mismatch: stale or missing "
            "Parquet objects remain"
        )

    root_inventory_after = list_gcs_parquet_inventory(destination_root)
    unrelated_after = {
        item for item in root_inventory_after if not item.startswith(target_prefix)
    }
    if unrelated_after != unrelated_before:
        raise RuntimeError(
            "An unrelated event_date partition changed during incremental sync"
        )

    print(
        f"Partition sync verified: remote parquet files={len(remote_inventory)}, "
        f"unchanged unrelated parquet objects={len(unrelated_after)}"
    )


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Mirror one validated processed event_date partition to GCS."
    )
    parser.add_argument("--event-date", required=True, type=parse_event_date)
    parser.add_argument(
        "--scope",
        choices=sorted(SCOPES),
        default="canonical",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    sync_partition(args.event_date, args.scope)


if __name__ == "__main__":
    main()
