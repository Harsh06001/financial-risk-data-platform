import argparse
import random
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from generate_transactions import generate_dataset
from validate_transactions import validate_file


PROJECT_ROOT = Path(__file__).resolve().parents[1]

TERRAFORM_DIR = PROJECT_ROOT / "infrastructure" / "terraform"

DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "raw" / "transactions"


def get_raw_bucket_name() -> str:
    command = [
        "terraform",
        f"-chdir={TERRAFORM_DIR}",
        "output",
        "-raw",
        "raw_data_bucket_name",
    ]

    result = subprocess.run(
        command,
        check=True,
        capture_output=True,
        text=True,
    )

    bucket_name = result.stdout.strip()

    if not bucket_name:
        raise RuntimeError("Terraform returned an empty bucket name.")

    return bucket_name


def upload_to_gcs(input_path: Path, bucket_name: str) -> str:
    ingestion_date = datetime.now(timezone.utc).date().isoformat()

    destination = (
        f"gs://{bucket_name}/"
        f"transactions/ingestion_date={ingestion_date}/"
    )

    command = [
        "gcloud",
        "storage",
        "cp",
        str(input_path),
        destination,
    ]

    subprocess.run(
        command,
        check=True,
    )

    return f"{destination}{input_path.name}"


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Generate, validate, and upload synthetic "
            "financial transactions to GCS."
        )
    )

    parser.add_argument(
        "--rows",
        type=int,
        default=100,
        help="Number of transactions to generate.",
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible transaction generation.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_arguments()

    if args.rows <= 0:
        print("Ingestion failed: --rows must be greater than zero.")
        sys.exit(1)

    print("STEP 1: GENERATE")
    print("-" * 40)

    random.seed(args.seed)

    output_path = generate_dataset(
        row_count=args.rows,
        output_dir=DEFAULT_OUTPUT_DIR,
    )

    print()
    print("STEP 2: VALIDATE")
    print("-" * 40)

    validation_passed = validate_file(output_path)

    if not validation_passed:
        print()
        print("INGESTION STOPPED: validation failed.")
        sys.exit(1)

    print()
    print("STEP 3: RESOLVE INFRASTRUCTURE")
    print("-" * 40)

    bucket_name = get_raw_bucket_name()

    print(f"Raw bucket: {bucket_name}")

    print()
    print("STEP 4: UPLOAD")
    print("-" * 40)

    object_uri = upload_to_gcs(
        input_path=output_path,
        bucket_name=bucket_name,
    )

    print()
    print("INGESTION COMPLETE")
    print(f"Local file: {output_path}")
    print(f"Cloud object: {object_uri}")


if __name__ == "__main__":
    main()