import subprocess
from pathlib import Path


def run_command(command: list[str], cwd: Path | None = None) -> None:
    print(f"\nRunning: {' '.join(command)}")
    subprocess.run(command, cwd=str(cwd) if cwd else None, check=True)


def count_local_parquet_files(path: Path) -> int:
    return sum(1 for _ in path.rglob("*.parquet"))


def list_local_parquet_inventory(path: Path) -> set[str]:
    return {item.relative_to(path).as_posix() for item in path.rglob("*.parquet")}


def list_event_date_partitions(parquet_inventory: set[str]) -> set[str]:
    return {
        relative_path.split("/", 1)[0]
        for relative_path in parquet_inventory
        if relative_path.startswith("event_date=") and "/" in relative_path
    }


def list_gcs_parquet_inventory(destination_uri: str) -> set[str]:
    command = [
        "gcloud",
        "storage",
        "ls",
        "--recursive",
        destination_uri,
    ]

    completed = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=True,
    )

    prefix = destination_uri.rstrip("/") + "/"
    inventory: set[str] = set()

    for line in completed.stdout.splitlines():
        item = line.strip()
        if not item.endswith(".parquet"):
            continue
        if not item.startswith(prefix):
            raise RuntimeError(f"Unexpected GCS object {item} for prefix {destination_uri}")
        inventory.add(item[len(prefix):])

    return inventory


def validate_local_source(source_path: Path, expected_root: Path, description: str) -> Path:
    if not source_path.exists():
        raise FileNotFoundError(f"{description} does not exist: {source_path}")

    resolved_source = source_path.resolve(strict=True)
    resolved_root = expected_root.resolve(strict=True)

    if not resolved_source.is_dir():
        raise ValueError(f"Expected {description} to be a directory: {source_path}")

    if resolved_source != resolved_root and resolved_root not in resolved_source.parents:
        raise ValueError(
            f"{description} must resolve within the expected local root {expected_root}: {source_path}"
        )

    return resolved_source


def validate_exact_local_source(
    source_path: Path,
    expected_path: Path,
    description: str,
) -> Path:
    resolved_source = validate_local_source(source_path, expected_path, description)
    resolved_expected = expected_path.absolute()

    if resolved_source != resolved_expected:
        raise ValueError(
            f"{description} must resolve exactly to {resolved_expected}: {source_path}"
        )

    return resolved_source


def validate_gcs_destination(destination_uri: str, expected_prefix: str, description: str) -> str:
    if not destination_uri or not destination_uri.strip():
        raise ValueError(f"{description} destination is empty")

    normalized_destination = destination_uri.strip().rstrip("/")

    if not normalized_destination.startswith("gs://"):
        raise ValueError(f"{description} destination must start with gs://: {destination_uri}")

    if normalized_destination in {
        "gs://risk-data-platform-npg-2026-processed-data",
        "gs://risk-data-platform-npg-2026-analytics-data",
    }:
        raise ValueError(f"{description} destination cannot target the bucket root")

    expected = expected_prefix.strip().rstrip("/")
    if normalized_destination != expected:
        raise ValueError(
            f"{description} destination must be exactly {expected}; received {normalized_destination}"
        )

    return normalized_destination
