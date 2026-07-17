"""Idempotently stage and MERGE local streaming silver Parquet into BigQuery."""

import argparse
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "risk-data-platform-npg-2026")
DATASET_ID = os.environ.get("DBT_DATASET", "risk_analytics")
LOCATION = os.environ.get("GCP_LOCATION", "us-central1")
TARGET_TABLE = "streaming_transaction_events"
STAGE_TABLE = "streaming_transaction_events_stage"


def run_command(command: list[str], capture_output: bool = False) -> str:
    completed = subprocess.run(
        command,
        check=True,
        text=True,
        capture_output=capture_output,
    )
    return completed.stdout


def load_streaming_events(silver_root: Path) -> dict[str, object]:
    parquet_files = sorted(str(path) for path in silver_root.rglob("*.parquet"))
    if not parquet_files:
        raise RuntimeError(f"No streaming silver Parquet files found under {silver_root}")

    prefix = [
        "bq",
        f"--project_id={PROJECT_ID}",
        f"--location={LOCATION}",
    ]
    for index, parquet_file in enumerate(parquet_files):
        load_flags = ["--replace"] if index == 0 else []
        run_command(
            prefix
            + [
                "load",
                *load_flags,
                "--source_format=PARQUET",
                f"{DATASET_ID}.{STAGE_TABLE}",
                parquet_file,
            ]
        )
    query = f"""
    CREATE TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID}.{TARGET_TABLE}`
    PARTITION BY event_date AS
    SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.{STAGE_TABLE}` WHERE FALSE;

    MERGE `{PROJECT_ID}.{DATASET_ID}.{TARGET_TABLE}` target
    USING (
      SELECT * EXCEPT(row_number)
      FROM (
        SELECT *, ROW_NUMBER() OVER (
          PARTITION BY transaction_id
          ORDER BY ingestion_timestamp DESC, kafka_offset DESC
        ) AS row_number
        FROM `{PROJECT_ID}.{DATASET_ID}.{STAGE_TABLE}`
      )
      WHERE row_number = 1
    ) source
    ON target.transaction_id = source.transaction_id
    WHEN MATCHED THEN UPDATE SET
      event_timestamp = source.event_timestamp,
      event_date = source.event_date,
      event_hour = source.event_hour,
      customer_id = source.customer_id,
      merchant_id = source.merchant_id,
      amount = source.amount,
      currency = source.currency,
      country = source.country,
      merchant_category = source.merchant_category,
      payment_method = source.payment_method,
      device_id = source.device_id,
      event_type = source.event_type,
      ingestion_timestamp = source.ingestion_timestamp,
      is_late = source.is_late,
      processing_batch_id = source.processing_batch_id,
      processing_run_id = source.processing_run_id,
      raw_json = source.raw_json,
      topic = source.topic,
      kafka_partition = source.kafka_partition,
      kafka_offset = source.kafka_offset,
      kafka_timestamp = source.kafka_timestamp,
      unexpected_field_count = source.unexpected_field_count
    WHEN NOT MATCHED THEN INSERT ROW;
    """
    run_command(prefix + ["query", "--use_legacy_sql=false", query])
    metrics_raw = run_command(
        prefix
        + [
            "query",
            "--use_legacy_sql=false",
            "--format=prettyjson",
            f"SELECT COUNT(*) row_count, COUNT(DISTINCT transaction_id) unique_ids FROM `{PROJECT_ID}.{DATASET_ID}.{TARGET_TABLE}`",
        ],
        capture_output=True,
    )
    metrics = json.loads(metrics_raw)[0]
    if int(metrics["row_count"]) != int(metrics["unique_ids"]):
        raise RuntimeError(f"BigQuery streaming target contains duplicates: {metrics}")
    print(f"STREAMING BIGQUERY LOAD PASSED: {metrics}")
    return metrics


def write_status(output: Path, success: bool, details: object) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(
            {
                "observation_timestamp": datetime.now(timezone.utc).isoformat(),
                "success": success,
                "details": details,
            },
            indent=2,
            default=str,
        )
        + "\n",
        encoding="utf-8",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Load streaming silver data to BigQuery.")
    parser.add_argument(
        "--silver-root",
        type=Path,
        default=Path("data/streaming/silver/transaction_events"),
    )
    parser.add_argument(
        "--status-output",
        type=Path,
        default=Path("data/streaming/load-status/bigquery.json"),
    )
    args = parser.parse_args()
    try:
        metrics = load_streaming_events(args.silver_root)
    except Exception as exc:
        write_status(args.status_output, False, f"{type(exc).__name__}: {exc}")
        raise
    write_status(args.status_output, True, metrics)


if __name__ == "__main__":
    main()
