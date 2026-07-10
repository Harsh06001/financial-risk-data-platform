import argparse
from datetime import date

from load_bigquery_tables import (
    DATASET_ID,
    LOCATION,
    PROCESSED_TRANSACTIONS_URI,
    PROJECT_ID,
    run_command,
)
from validate_bigquery_tables import run_query


SCOPES = {
    "canonical": {
        "source_root": PROCESSED_TRANSACTIONS_URI,
        "stage": "processed_transactions_incremental_stage",
        "target": "processed_transactions",
    },
    "demo": {
        "source_root": (
            "gs://risk-data-platform-npg-2026-processed-data/"
            "incremental-demo/transactions"
        ),
        "stage": "processed_transactions_incremental_demo_stage",
        "target": "processed_transactions_incremental_demo",
    },
}

MERGE_COLUMNS = [
    "event_timestamp",
    "event_date",
    "event_hour",
    "customer_id",
    "merchant_id",
    "amount",
    "currency",
    "country",
    "merchant_category",
    "payment_method",
    "device_id",
    "is_fraud",
    "source_file",
    "processed_at",
]


def parse_event_date(value: str) -> str:
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "event date must use YYYY-MM-DD format"
        ) from exc


def table_metrics(table_name: str) -> dict[str, int]:
    query = f"""
    SELECT
      COUNT(*) AS row_count,
      COUNT(DISTINCT transaction_id) AS unique_transaction_ids
    FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`
    """
    row = run_query(query)[0]
    return {
        "row_count": int(row["row_count"]),
        "unique_transaction_ids": int(row["unique_transaction_ids"]),
    }


def load_stage(event_date: str, scope: str) -> str:
    config = SCOPES[scope]
    stage = str(config["stage"])
    source_root = str(config["source_root"])
    run_command(
        [
            "bq",
            f"--project_id={PROJECT_ID}",
            f"--location={LOCATION}",
            "load",
            "--replace",
            "--source_format=PARQUET",
            "--hive_partitioning_mode=AUTO",
            f"--hive_partitioning_source_uri_prefix={source_root}/",
            f"{DATASET_ID}.{stage}",
            f"{source_root}/event_date={event_date}/*.parquet",
        ]
    )
    return stage


def validate_stage(stage: str, event_date: str) -> int:
    query = f"""
    SELECT
      COUNT(*) AS row_count,
      COUNT(DISTINCT transaction_id) AS unique_transaction_ids,
      COUNTIF(transaction_id IS NULL) AS null_transaction_ids,
      COUNTIF(event_date IS NULL OR event_date != DATE('{event_date}')) AS wrong_date_rows
    FROM `{PROJECT_ID}.{DATASET_ID}.{stage}`
    """
    row = run_query(query)[0]
    row_count = int(row["row_count"])

    if row_count == 0:
        raise RuntimeError("Incremental staging table contains no rows")
    if int(row["unique_transaction_ids"]) != row_count:
        raise RuntimeError("Incremental staging table contains duplicate transaction IDs")
    if int(row["null_transaction_ids"]) != 0:
        raise RuntimeError("Incremental staging table contains null transaction IDs")
    if int(row["wrong_date_rows"]) != 0:
        raise RuntimeError("Incremental staging table contains an unexpected event_date")

    return row_count


def merge_stage(stage: str, target: str) -> None:
    update_assignments = ",\n      ".join(
        f"{column} = source.{column}" for column in MERGE_COLUMNS
    )
    insert_columns = ["transaction_id", *MERGE_COLUMNS]
    insert_column_sql = ", ".join(insert_columns)
    insert_value_sql = ", ".join(
        f"source.{column}" for column in insert_columns
    )
    query = f"""
    MERGE `{PROJECT_ID}.{DATASET_ID}.{target}` AS target
    USING `{PROJECT_ID}.{DATASET_ID}.{stage}` AS source
    ON target.transaction_id = source.transaction_id
    WHEN MATCHED THEN UPDATE SET
      {update_assignments}
    WHEN NOT MATCHED THEN INSERT ({insert_column_sql})
    VALUES ({insert_value_sql})
    """
    run_command(
        [
            "bq",
            f"--project_id={PROJECT_ID}",
            f"--location={LOCATION}",
            "query",
            "--use_legacy_sql=false",
            query,
        ]
    )


def merge_processed_partition(event_date: str, scope: str) -> dict[str, int]:
    target = str(SCOPES[scope]["target"])
    before = table_metrics(target)
    stage = load_stage(event_date, scope)
    staged_rows = validate_stage(stage, event_date)
    merge_stage(stage, target)
    after = table_metrics(target)

    if after["row_count"] != after["unique_transaction_ids"]:
        raise RuntimeError("MERGE target contains duplicate transaction IDs")

    result = {
        "before_rows": before["row_count"],
        "staged_rows": staged_rows,
        "after_rows": after["row_count"],
        "unique_transaction_ids": after["unique_transaction_ids"],
    }
    print(
        "MERGE_RESULT "
        + " ".join(f"{key}={value}" for key, value in result.items())
    )
    return result


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stage and MERGE one processed event_date into BigQuery."
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
    merge_processed_partition(args.event_date, args.scope)


if __name__ == "__main__":
    main()
