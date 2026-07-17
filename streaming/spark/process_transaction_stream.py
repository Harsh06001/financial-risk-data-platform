"""Bounded Spark Structured Streaming consumer for transaction events."""

import argparse
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
DEFAULT_DATA_ROOT = PROJECT_ROOT / "data" / "streaming"


def env_flag(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process Kafka transaction events.")
    parser.add_argument(
        "--bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:19092"),
    )
    parser.add_argument(
        "--topic", default=os.environ.get("KAFKA_TOPIC", "transaction-events")
    )
    parser.add_argument(
        "--dlq-topic",
        default=os.environ.get("KAFKA_DLQ_TOPIC", "transaction-events-dlq"),
    )
    parser.add_argument(
        "--risk-alert-topic",
        default=os.environ.get("KAFKA_RISK_ALERT_TOPIC", "streaming-risk-alerts"),
    )
    parser.add_argument(
        "--publish-dlq",
        action=argparse.BooleanOptionalAction,
        default=env_flag("STREAM_PUBLISH_DLQ", True),
    )
    parser.add_argument(
        "--publish-risk-alerts",
        action=argparse.BooleanOptionalAction,
        default=env_flag("STREAM_PUBLISH_RISK_ALERTS", True),
    )
    parser.add_argument(
        "--risk-alert-amount",
        type=float,
        default=float(os.environ.get("STREAM_RISK_ALERT_AMOUNT", "1000")),
    )
    parser.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT)
    parser.add_argument("--starting-offsets", choices=["earliest", "latest"], default="earliest")
    parser.add_argument("--available-now", action="store_true")
    parser.add_argument("--max-runtime-seconds", type=int, default=60)
    parser.add_argument("--late-threshold-hours", type=int, default=24)
    parser.add_argument("--run-id", default=str(uuid.uuid4()))
    return parser.parse_args()


def build_event_schema():
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType

    return StructType(
        [
            StructField("transaction_id", StringType(), True),
            StructField("event_timestamp", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("merchant_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
            StructField("country", StringType(), True),
            StructField("merchant_category", StringType(), True),
            StructField("payment_method", StringType(), True),
            StructField("device_id", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("ingestion_timestamp", StringType(), True),
        ]
    )


def main() -> None:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        col,
        array,
        array_except,
        expr,
        from_json,
        hour,
        lit,
        map_keys,
        coalesce,
        size,
        to_date,
        to_timestamp,
        when,
    )
    from pyspark.sql.types import MapType, StringType

    from streaming.contracts import EVENT_FIELDS

    args = parse_arguments()
    if args.max_runtime_seconds < 1:
        raise ValueError("max-runtime-seconds must be at least 1")
    if args.late_threshold_hours < 0:
        raise ValueError("late-threshold-hours must be non-negative")
    if args.risk_alert_amount <= 0:
        raise ValueError("risk-alert-amount must be positive")

    data_root = args.data_root.resolve()
    bronze_root = data_root / "bronze" / "transaction_events"
    silver_root = data_root / "silver" / "transaction_events"
    quarantine_root = data_root / "quarantine" / "transaction_events"
    metrics_root = data_root / "metrics" / "transaction_events"
    checkpoint_root = data_root / "checkpoints" / "transaction_events"
    metrics_root.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder.appName("FinancialRiskTransactionStream")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "true")
        .load()
    )
    decoded = kafka_stream.select(
        col("value").cast("string").alias("raw_json"),
        col("topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
    )
    parsed = (
        decoded.withColumn("event", from_json(col("raw_json"), build_event_schema()))
        .withColumn(
            "raw_map",
            from_json(col("raw_json"), MapType(StringType(), StringType())),
        )
        .withColumn(
            "unexpected_field_count",
            size(
                array_except(
                    map_keys(col("raw_map")),
                    array(*[lit(field) for field in EVENT_FIELDS]),
                )
            ),
        )
        .select(
            "raw_json",
            "topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "unexpected_field_count",
            "event.*",
        )
    )
    typed = (
        parsed.withColumn("event_timestamp", to_timestamp("event_timestamp"))
        .withColumn("ingestion_timestamp", to_timestamp("ingestion_timestamp"))
        .withColumn("event_date", to_date("event_timestamp"))
        .withColumn("event_hour", hour("event_timestamp"))
    )
    required = [
        "transaction_id",
        "event_timestamp",
        "ingestion_timestamp",
        "customer_id",
        "merchant_id",
        "currency",
        "country",
        "merchant_category",
        "payment_method",
        "device_id",
    ]
    valid_condition = (
        col("amount").isNotNull()
        & (col("amount") > 0)
        & (col("unexpected_field_count") == 0)
    )
    for field in required:
        valid_condition = valid_condition & col(field).isNotNull()
    valid_condition = valid_condition & col("event_type").isin(
        "authorization", "purchase", "refund"
    )
    prepared = (
        typed.withColumn("is_valid", coalesce(valid_condition, lit(False)))
        .withColumn(
            "validation_error",
            when(valid_condition, lit(None)).otherwise(
                lit("schema_or_value_contract_failed")
            ),
        )
        .withColumn(
            "is_late",
            expr(
                "event_timestamp < ingestion_timestamp "
                f"- INTERVAL {args.late_threshold_hours} HOURS"
            ),
        )
    )

    def process_batch(batch_df, batch_id: int) -> None:
        started = time.monotonic()
        batch_df = batch_df.cache()
        try:
            input_count = batch_df.count()
            valid_df = batch_df.filter(col("is_valid"))
            invalid_df = batch_df.filter(~col("is_valid"))
            valid_count = valid_df.count()
            invalid_count = input_count - valid_count
            clean_df = valid_df.dropDuplicates(["transaction_id"])
            clean_count = clean_df.count()
            duplicate_count = valid_count - clean_count
            late_count = clean_df.filter(col("is_late")).count()
            schema_drift_count = batch_df.filter(
                col("unexpected_field_count") > 0
            ).count()
            risk_df = clean_df.filter(col("amount") >= args.risk_alert_amount)
            risk_alert_count = risk_df.count()

            enriched_clean = (
                clean_df.withColumn("processing_batch_id", lit(batch_id))
                .withColumn("processing_run_id", lit(args.run_id))
                .drop("is_valid", "validation_error")
            )
            batch_df.write.mode("overwrite").parquet(
                str(bronze_root / f"batch_id={batch_id}")
            )
            enriched_clean.write.mode("overwrite").parquet(
                str(silver_root / f"batch_id={batch_id}")
            )
            invalid_df.write.mode("overwrite").parquet(
                str(quarantine_root / f"batch_id={batch_id}")
            )

            dlq_count = 0
            if args.publish_dlq and invalid_count:
                (
                    invalid_df.select(
                        col("transaction_id").cast("string").alias("key"),
                        col("raw_json").cast("string").alias("value"),
                    )
                    .write.format("kafka")
                    .option("kafka.bootstrap.servers", args.bootstrap_servers)
                    .option("topic", args.dlq_topic)
                    .save()
                )
                dlq_count = invalid_count

            published_risk_alert_count = 0
            if args.publish_risk_alerts and risk_alert_count:
                (
                    risk_df.select(
                        col("transaction_id").cast("string").alias("key"),
                        col("raw_json").cast("string").alias("value"),
                    )
                    .write.format("kafka")
                    .option("kafka.bootstrap.servers", args.bootstrap_servers)
                    .option("topic", args.risk_alert_topic)
                    .save()
                )
                published_risk_alert_count = risk_alert_count

            metrics = {
                "pipeline_name": "transaction_event_stream",
                "processing_run_id": args.run_id,
                "batch_id": int(batch_id),
                "input_count": input_count,
                "valid_count": valid_count,
                "clean_count": clean_count,
                "invalid_count": invalid_count,
                "duplicate_count": duplicate_count,
                "late_count": late_count,
                "schema_drift_count": schema_drift_count,
                "dlq_count": dlq_count,
                "risk_alert_count": published_risk_alert_count,
                "processing_timestamp": datetime.now(timezone.utc).isoformat(),
                "processing_duration_seconds": round(time.monotonic() - started, 6),
                "reconciliation": input_count
                == clean_count + invalid_count + duplicate_count,
            }
            metrics_path = metrics_root / f"batch_id={batch_id}.json"
            metrics_path.write_text(
                json.dumps(metrics, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            print(f"STREAM BATCH COMPLETE: {metrics}")
        finally:
            batch_df.unpersist()

    writer = prepared.writeStream.foreachBatch(process_batch).option(
        "checkpointLocation", str(checkpoint_root)
    )
    if args.available_now:
        query = writer.trigger(availableNow=True).start()
        query.awaitTermination()
    else:
        query = writer.trigger(processingTime="5 seconds").start()
        query.awaitTermination(args.max_runtime_seconds)
        if query.isActive:
            query.stop()
    spark.stop()


if __name__ == "__main__":
    main()
