from pathlib import Path

from sync_utils import (
    list_gcs_parquet_inventory,
    list_event_date_partitions,
    list_local_parquet_inventory,
    run_command,
    validate_exact_local_source,
    validate_gcs_destination,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LOCAL_PROCESSED_ROOT = PROJECT_ROOT / "data" / "processed" / "transactions"
EXPECTED_GCS_DESTINATION = "gs://risk-data-platform-npg-2026-processed-data/transactions"


def preflight_processed_sync() -> tuple[Path, str, set[str]]:
    source_path = validate_exact_local_source(
        LOCAL_PROCESSED_ROOT,
        LOCAL_PROCESSED_ROOT,
        "processed data source",
    )
    destination_uri = validate_gcs_destination(
        EXPECTED_GCS_DESTINATION,
        EXPECTED_GCS_DESTINATION,
        "processed GCS prefix",
    )

    local_parquet_inventory = list_local_parquet_inventory(source_path)
    local_partitions = list_event_date_partitions(local_parquet_inventory)

    if not local_parquet_inventory:
        raise RuntimeError("Processed source contains no Parquet files; refusing destructive sync")

    if len(local_parquet_inventory) != 31:
        raise RuntimeError(
            "Expected 31 local processed Parquet files before sync, "
            f"found {len(local_parquet_inventory)}"
        )

    if len(local_partitions) != 31:
        raise RuntimeError(
            "Expected 31 local event_date partitions before sync, "
            f"found {len(local_partitions)}"
        )

    print(
        "Processed preflight passed: "
        f"local parquet files={len(local_parquet_inventory)}, "
        f"event_date partitions={len(local_partitions)}"
    )

    return source_path, destination_uri, local_parquet_inventory


def verify_processed_sync(
    destination_uri: str,
    local_parquet_inventory: set[str],
) -> None:
    remote_parquet_inventory = list_gcs_parquet_inventory(destination_uri)
    remote_partitions = list_event_date_partitions(remote_parquet_inventory)

    print(f"GCS parquet files: {len(remote_parquet_inventory)}")
    print(f"GCS event-date prefixes: {len(remote_partitions)}")

    if len(remote_parquet_inventory) != 31:
        raise RuntimeError(f"Expected 31 GCS parquet objects, found {len(remote_parquet_inventory)}")

    if len(remote_partitions) != 31:
        raise RuntimeError(
            f"Expected 31 GCS event_date partitions, found {len(remote_partitions)}"
        )

    if remote_parquet_inventory != local_parquet_inventory:
        raise RuntimeError("Processed GCS parquet inventory does not match the local output")


def main() -> None:
    source_path, destination_uri, local_parquet_inventory = (
        preflight_processed_sync()
    )

    print("[SYNC] Processed data to GCS")
    print(f"Local source: {source_path}")
    print(f"Cloud destination: {destination_uri}")

    command = [
        "gcloud",
        "storage",
        "rsync",
        "--recursive",
        "--delete-unmatched-destination-objects",
        str(source_path),
        destination_uri,
    ]

    run_command(command, cwd=PROJECT_ROOT)
    verify_processed_sync(destination_uri, local_parquet_inventory)
    print("Processed sync completed successfully.")


if __name__ == "__main__":
    main()
