import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    max as spark_max,
    min as spark_min,
)


EXPECTED_SCHEMA = {
    "transaction_id": "string",
    "event_timestamp": "timestamp",
    "customer_id": "string",
    "merchant_id": "string",
    "amount": "double",
    "currency": "string",
    "country": "string",
    "merchant_category": "string",
    "payment_method": "string",
    "device_id": "string",
    "is_fraud": "boolean",
    "event_hour": "int",
    "source_file": "string",
    "processed_at": "timestamp",
    "event_date": "date",
}


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FinancialRiskProcessedValidation")
        .master("local[*]")
        .getOrCreate()
    )


def count_rows(
    transactions_df: DataFrame,
    condition,
) -> int:
    return transactions_df.filter(condition).count()


def validate_schema(transactions_df: DataFrame) -> list[str]:
    errors: list[str] = []

    actual_schema = {
        field.name: field.dataType.simpleString()
        for field in transactions_df.schema.fields
    }

    missing_columns = [
        column
        for column in EXPECTED_SCHEMA
        if column not in actual_schema
    ]

    extra_columns = [
        column
        for column in actual_schema
        if column not in EXPECTED_SCHEMA
    ]

    if missing_columns:
        errors.append(
            "Missing columns: "
            + ", ".join(missing_columns)
        )

    if extra_columns:
        errors.append(
            "Unexpected columns: "
            + ", ".join(extra_columns)
        )

    for column, expected_type in EXPECTED_SCHEMA.items():
        actual_type = actual_schema.get(column)

        if actual_type and actual_type != expected_type:
            errors.append(
                f"Column {column} has type {actual_type}; "
                f"expected {expected_type}."
            )

    return errors


def read_partition_dates(input_path: Path) -> set[str]:
    return {
        path.name.split("=", maxsplit=1)[1]
        for path in input_path.iterdir()
        if path.is_dir()
        and path.name.startswith("event_date=")
    }


def read_dataframe_dates(transactions_df: DataFrame) -> set[str]:
    return {
        row.event_date.isoformat()
        for row in transactions_df.select("event_date").distinct().collect()
        if row.event_date is not None
    }


def validate_processed_data(
    spark: SparkSession,
    input_path: Path,
    expected_rows: int | None,
    expected_event_dates: int | None,
) -> bool:
    errors: list[str] = []

    transactions_df = spark.read.parquet(str(input_path))

    schema_errors = validate_schema(transactions_df)

    print("PROCESSED DATA VALIDATION")
    print("-" * 50)
    print(f"Input path: {input_path}")

    if schema_errors:
        print()
        print("SCHEMA CHECK")
        print("-" * 50)

        for error in schema_errors:
            print(f"- {error}")

        print()
        print("VALIDATION FAILED")
        return False

    total_rows = transactions_df.count()

    unique_transaction_ids = (
        transactions_df
        .select("transaction_id")
        .distinct()
        .count()
    )

    event_date_count = (
        transactions_df
        .select("event_date")
        .distinct()
        .count()
    )

    date_bounds = transactions_df.select(
        spark_min("event_date").alias("min_event_date"),
        spark_max("event_date").alias("max_event_date"),
    ).first()

    fraud_rows = count_rows(
        transactions_df,
        col("is_fraud"),
    )

    null_transaction_ids = count_rows(
        transactions_df,
        col("transaction_id").isNull(),
    )

    null_event_timestamps = count_rows(
        transactions_df,
        col("event_timestamp").isNull(),
    )

    null_event_dates = count_rows(
        transactions_df,
        col("event_date").isNull(),
    )

    invalid_amounts = count_rows(
        transactions_df,
        col("amount").isNull()
        | (col("amount") <= 0),
    )

    invalid_event_hours = count_rows(
        transactions_df,
        col("event_hour").isNull()
        | (col("event_hour") < 0)
        | (col("event_hour") > 23),
    )

    null_source_files = count_rows(
        transactions_df,
        col("source_file").isNull(),
    )

    partition_dates = read_partition_dates(input_path)
    dataframe_dates = read_dataframe_dates(transactions_df)

    parquet_file_count = sum(
        1
        for _ in input_path.rglob("*.parquet")
    )

    print(f"Total rows: {total_rows:,}")
    print(f"Unique transaction IDs: {unique_transaction_ids:,}")
    print(f"Fraud rows: {fraud_rows:,}")
    print(f"Distinct event dates: {event_date_count:,}")
    print(
        f"Date range: "
        f"{date_bounds.min_event_date} to {date_bounds.max_event_date}"
    )
    print(f"Physical event_date directories: {len(partition_dates):,}")
    print(f"Parquet files: {parquet_file_count:,}")

    if total_rows == 0:
        errors.append("Processed dataset contains no rows.")

    if expected_rows is not None and total_rows != expected_rows:
        errors.append(
            f"Expected {expected_rows:,} rows; found {total_rows:,}."
        )

    if unique_transaction_ids != total_rows:
        duplicate_count = total_rows - unique_transaction_ids
        errors.append(
            f"Found {duplicate_count:,} duplicate transaction_id rows."
        )

    if (
        expected_event_dates is not None
        and event_date_count != expected_event_dates
    ):
        errors.append(
            f"Expected {expected_event_dates:,} event dates; "
            f"found {event_date_count:,}."
        )

    if null_transaction_ids:
        errors.append(
            f"Found {null_transaction_ids:,} null transaction_id values."
        )

    if null_event_timestamps:
        errors.append(
            f"Found {null_event_timestamps:,} null event_timestamp values."
        )

    if null_event_dates:
        errors.append(
            f"Found {null_event_dates:,} null event_date values."
        )

    if invalid_amounts:
        errors.append(
            f"Found {invalid_amounts:,} null or non-positive amount values."
        )

    if invalid_event_hours:
        errors.append(
            f"Found {invalid_event_hours:,} invalid event_hour values."
        )

    if null_source_files:
        errors.append(
            f"Found {null_source_files:,} null source_file values."
        )

    if partition_dates != dataframe_dates:
        missing_dirs = sorted(dataframe_dates - partition_dates)
        extra_dirs = sorted(partition_dates - dataframe_dates)

        if missing_dirs:
            errors.append(
                "Missing physical event_date directories for: "
                + ", ".join(missing_dirs)
            )

        if extra_dirs:
            errors.append(
                "Unexpected physical event_date directories for: "
                + ", ".join(extra_dirs)
            )

    print()

    if errors:
        print("VALIDATION FAILED")
        print(f"Errors found: {len(errors)}")

        for error in errors:
            print(f"- {error}")

        return False

    print("VALIDATION PASSED")
    return True


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate processed transaction Parquet output."
    )

    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/processed/transactions"),
        help="Path to processed transaction Parquet output.",
    )

    parser.add_argument(
        "--expected-rows",
        type=int,
        default=None,
        help="Optional expected processed row count.",
    )

    parser.add_argument(
        "--expected-event-dates",
        type=int,
        default=None,
        help="Optional expected distinct event_date count.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_arguments()

    if not args.input.exists():
        print(f"Input path does not exist: {args.input}")
        sys.exit(1)

    if not args.input.is_dir():
        print(f"Input path is not a directory: {args.input}")
        sys.exit(1)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        validation_passed = validate_processed_data(
            spark=spark,
            input_path=args.input,
            expected_rows=args.expected_rows,
            expected_event_dates=args.expected_event_dates,
        )
    finally:
        spark.stop()

    if not validation_passed:
        sys.exit(1)


if __name__ == "__main__":
    main()
