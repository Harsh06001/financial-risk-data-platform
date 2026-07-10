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
LOCAL_ANALYTICS_ROOT = PROJECT_ROOT / "data" / "analytics" / "risk_features"
EXPECTED_GCS_DESTINATION = "gs://risk-data-platform-npg-2026-analytics-data/risk_features"
TABLES = [
    "daily_transaction_summary",
    "customer_risk_features",
    "merchant_risk_features",
    "segment_risk_summary",
    "high_risk_transactions",
]
EXPECTED_PARQUET_FILES = {
    "daily_transaction_summary": 1,
    "customer_risk_features": 1,
    "merchant_risk_features": 1,
    "segment_risk_summary": 1,
    "high_risk_transactions": 31,
}


def sync_table(local_table_path: Path, remote_table_uri: str) -> None:
    print(f"Syncing {local_table_path} -> {remote_table_uri}")
    command = [
        "gcloud",
        "storage",
        "rsync",
        "--recursive",
        "--delete-unmatched-destination-objects",
        str(local_table_path),
        remote_table_uri,
    ]

    run_command(command, cwd=PROJECT_ROOT)


def preflight_analytics_sync() -> list[tuple[str, Path, str, set[str]]]:
    validate_exact_local_source(
        LOCAL_ANALYTICS_ROOT,
        LOCAL_ANALYTICS_ROOT,
        "analytics root",
    )
    targets: list[tuple[str, Path, str, set[str]]] = []

    for table_name in TABLES:
        local_table_path = LOCAL_ANALYTICS_ROOT / table_name
        local_table_path = validate_exact_local_source(
            local_table_path,
            local_table_path,
            f"analytics table {table_name}",
        )
        remote_table_uri = validate_gcs_destination(
            f"{EXPECTED_GCS_DESTINATION}/{table_name}",
            f"{EXPECTED_GCS_DESTINATION}/{table_name}",
            f"analytics table {table_name}",
        )
        local_inventory = list_local_parquet_inventory(local_table_path)
        expected_count = EXPECTED_PARQUET_FILES[table_name]

        if not local_inventory:
            raise RuntimeError(
                f"Analytics table {table_name} contains no Parquet files; "
                "refusing destructive sync"
            )

        if len(local_inventory) != expected_count:
            raise RuntimeError(
                f"Expected {expected_count} local Parquet files for {table_name} "
                f"before sync, found {len(local_inventory)}"
            )

        partitions = list_event_date_partitions(local_inventory)
        if table_name == "high_risk_transactions" and len(partitions) != 31:
            raise RuntimeError(
                "Expected 31 local event_date partitions for high_risk_transactions "
                f"before sync, found {len(partitions)}"
            )

        if table_name != "high_risk_transactions" and partitions:
            raise RuntimeError(
                f"Analytics table {table_name} must not use event_date directories"
            )

        print(
            f"Analytics preflight passed for {table_name}: "
            f"local parquet files={len(local_inventory)}"
        )
        targets.append(
            (table_name, local_table_path, remote_table_uri, local_inventory)
        )

    return targets


def verify_sync(
    targets: list[tuple[str, Path, str, set[str]]],
) -> None:
    for table_name, _local_path, remote_table_uri, local_inventory in targets:
        remote_inventory = list_gcs_parquet_inventory(remote_table_uri)

        print(
            f"Verified {table_name}: local parquet files={len(local_inventory)}, "
            f"gcs parquet files={len(remote_inventory)}"
        )

        if remote_inventory != local_inventory:
            raise RuntimeError(f"Parquet inventory mismatch for {table_name}: stale or missing objects remain")


def main() -> None:
    targets = preflight_analytics_sync()

    print("[SYNC] Analytics to GCS")
    print(f"Local source: {LOCAL_ANALYTICS_ROOT}")
    print(f"Cloud destination: {EXPECTED_GCS_DESTINATION}")

    for table_name, local_table_path, remote_table_uri, _inventory in targets:
        sync_table(local_table_path, remote_table_uri)

    verify_sync(targets)
    print("Analytics sync completed successfully.")


if __name__ == "__main__":
    main()
