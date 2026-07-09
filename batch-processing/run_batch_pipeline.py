import argparse
import csv
import glob
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

DATA_GENERATOR_DIR = PROJECT_ROOT / "data-generator"
PROCESSING_SCRIPT = (
    PROJECT_ROOT
    / "batch-processing"
    / "process_transactions.py"
)

VALIDATION_SCRIPT = (
    PROJECT_ROOT
    / "batch-processing"
    / "validate_processed_transactions.py"
)

FEATURE_BUILD_SCRIPT = (
    PROJECT_ROOT
    / "batch-processing"
    / "build_risk_features.py"
)

FEATURE_VALIDATION_SCRIPT = (
    PROJECT_ROOT
    / "batch-processing"
    / "validate_risk_features.py"
)

sys.path.insert(0, str(DATA_GENERATOR_DIR))

from validate_transactions import validate_file  # noqa: E402


def resolve_raw_files(raw_input: str) -> list[Path]:
    raw_pattern = Path(raw_input)

    if not raw_pattern.is_absolute():
        raw_pattern = PROJECT_ROOT / raw_pattern

    return [
        Path(path)
        for path in sorted(glob.glob(str(raw_pattern)))
    ]


def count_csv_rows(input_path: Path) -> int:
    with input_path.open("r", newline="", encoding="utf-8") as csv_file:
        reader = csv.reader(csv_file)
        next(reader, None)

        return sum(1 for _ in reader)


def run_command(command: list[str]) -> None:
    print()
    print("COMMAND")
    print("-" * 50)
    print(" ".join(command))

    sys.stdout.flush()

    subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        check=True,
    )


def validate_raw_files(raw_files: list[Path]) -> int:
    total_rows = 0

    print("STEP 1: VALIDATE RAW CSV FILES")
    print("-" * 50)

    for raw_file in raw_files:
        validation_passed = validate_file(raw_file)

        if not validation_passed:
            raise RuntimeError(
                f"Raw validation failed for {raw_file}."
            )

        total_rows += count_csv_rows(raw_file)
        print()

    print(f"Validated raw files: {len(raw_files):,}")
    print(f"Validated raw rows: {total_rows:,}")

    return total_rows


def process_transactions(
    raw_input: str,
    output_path: Path,
    write_strategy: str,
) -> None:
    print()
    print("STEP 2: PROCESS TRANSACTIONS")
    print("-" * 50)

    command = [
        sys.executable,
        str(PROCESSING_SCRIPT),
        "--input",
        raw_input,
        "--output",
        str(output_path),
        "--write-strategy",
        write_strategy,
    ]

    run_command(command)


def validate_processed_output(
    output_path: Path,
    expected_rows: int,
    expected_event_dates: int | None,
) -> None:
    print()
    print("STEP 3: VALIDATE PROCESSED PARQUET")
    print("-" * 50)

    command = [
        sys.executable,
        str(VALIDATION_SCRIPT),
        "--input",
        str(output_path),
        "--expected-rows",
        str(expected_rows),
    ]

    if expected_event_dates is not None:
        command.extend(
            [
                "--expected-event-dates",
                str(expected_event_dates),
            ]
        )

    run_command(command)

def build_risk_features(
    processed_input: Path,
    feature_output: Path,
    high_value_threshold: float,
) -> None:
    print()
    print("STEP 4: BUILD RISK FEATURES")
    print("-" * 50)

    command = [
        sys.executable,
        str(FEATURE_BUILD_SCRIPT),
        "--input",
        str(processed_input),
        "--output",
        str(feature_output),
        "--high-value-threshold",
        str(high_value_threshold),
    ]

    run_command(command)

def validate_risk_features(
    feature_output: Path,
    expected_transactions: int,
    expected_event_dates: int | None,
    expected_customers: int | None,
    expected_merchants: int | None,
    expected_segments: int | None,
    expected_high_risk_transactions: int | None,
    high_value_threshold: float,
) -> None:
    print()
    print("STEP 5: VALIDATE RISK FEATURES")
    print("-" * 50)

    command = [
        sys.executable,
        str(FEATURE_VALIDATION_SCRIPT),
        "--input",
        str(feature_output),
        "--expected-transactions",
        str(expected_transactions),
        "--high-value-threshold",
        str(high_value_threshold),
    ]

    if expected_event_dates is not None:
        command.extend(
            [
                "--expected-event-dates",
                str(expected_event_dates),
            ]
        )

    if expected_customers is not None:
        command.extend(
            [
                "--expected-customers",
                str(expected_customers),
            ]
        )

    if expected_merchants is not None:
        command.extend(
            [
                "--expected-merchants",
                str(expected_merchants),
            ]
        )

    if expected_segments is not None:
        command.extend(
            [
                "--expected-segments",
                str(expected_segments),
            ]
        )

    if expected_high_risk_transactions is not None:
        command.extend(
            [
                "--expected-high-risk-transactions",
                str(expected_high_risk_transactions),
            ]
        )

    run_command(command)

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run the local batch transaction pipeline with raw and "
            "processed data validation."
        )
    )

    parser.add_argument(
        "--raw-input",
        default="data/raw/transactions/*.csv",
        help="Input path or glob for raw transaction CSV files.",
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/processed/transactions"),
        help="Output directory for processed Parquet data.",
    )

    parser.add_argument(
        "--write-strategy",
        choices=["baseline", "repartitioned"],
        default="repartitioned",
        help="Physical write strategy used for processed Parquet output.",
    )

    parser.add_argument(
        "--expected-event-dates",
        type=int,
        default=None,
        help="Optional expected distinct event_date count.",
    )

    parser.add_argument(
        "--feature-output",
        type=Path,
        default=Path("data/analytics/risk_features"),
        help="Output directory for generated risk feature tables.",
    )

    parser.add_argument(
        "--high-value-threshold",
        type=float,
        default=1000.0,
        help="Amount threshold used for high-value risk features.",
    )

    parser.add_argument(
        "--expected-customers",
        type=int,
        default=None,
        help="Optional expected number of customer feature rows.",
    )

    parser.add_argument(
        "--expected-merchants",
        type=int,
        default=None,
        help="Optional expected number of merchant feature rows.",
    )

    parser.add_argument(
        "--expected-segments",
        type=int,
        default=None,
        help="Optional expected number of segment summary rows.",
    )

    parser.add_argument(
        "--expected-high-risk-transactions",
        type=int,
        default=None,
        help="Optional expected number of high-risk transaction rows.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()

    raw_files = resolve_raw_files(args.raw_input)

    if not raw_files:
        print(f"No raw CSV files matched: {args.raw_input}")
        sys.exit(1)

    try:
        total_rows = validate_raw_files(raw_files)

        process_transactions(
            raw_input=args.raw_input,
            output_path=args.output,
            write_strategy=args.write_strategy,
        )

        validate_processed_output(
            output_path=args.output,
            expected_rows=total_rows,
            expected_event_dates=args.expected_event_dates,
        )
        build_risk_features(
            processed_input=args.output,
            feature_output=args.feature_output,
            high_value_threshold=args.high_value_threshold,
        )

        validate_risk_features(
            feature_output=args.feature_output,
            expected_transactions=total_rows,
            expected_event_dates=args.expected_event_dates,
            expected_customers=args.expected_customers,
            expected_merchants=args.expected_merchants,
            expected_segments=args.expected_segments,
            expected_high_risk_transactions=(
                args.expected_high_risk_transactions
            ),
            high_value_threshold=args.high_value_threshold,
        )
    except subprocess.CalledProcessError as error:
        print()
        print(f"BATCH PIPELINE FAILED: command exited with {error.returncode}")
        sys.exit(error.returncode)
    except RuntimeError as error:
        print()
        print(f"BATCH PIPELINE FAILED: {error}")
        sys.exit(1)

    print()
    print("BATCH PIPELINE COMPLETE")
    print(f"Processed output: {args.output}")
    print(f"Risk feature output: {args.feature_output}")


if __name__ == "__main__":
    main()
