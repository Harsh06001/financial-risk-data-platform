import argparse
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg as spark_avg,
    col,
    count as spark_count,
    countDistinct,
    current_timestamp,
    max as spark_max,
    min as spark_min,
    sum as spark_sum,
    when,
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FinancialRiskFeatureBuild")
        .master("local[*]")
        .getOrCreate()
    )


def fraud_count_column():
    return spark_sum(col("is_fraud").cast("int")).alias("fraud_count")


def fraud_rate_column():
    return spark_avg(col("is_fraud").cast("double")).alias("fraud_rate")


def high_value_count_column(high_value_threshold: float):
    return spark_sum(
        when(
            col("amount") >= high_value_threshold,
            1,
        ).otherwise(0)
    ).alias("high_value_transaction_count")


def add_generation_timestamp(features_df: DataFrame) -> DataFrame:
    return features_df.withColumn(
        "feature_generated_at",
        current_timestamp(),
    )


def build_daily_transaction_summary(
    transactions_df: DataFrame,
    high_value_threshold: float,
) -> DataFrame:
    return add_generation_timestamp(
        transactions_df
        .groupBy("event_date")
        .agg(
            spark_count("*").alias("transaction_count"),
            spark_sum("amount").alias("total_amount"),
            spark_avg("amount").alias("avg_amount"),
            spark_min("amount").alias("min_amount"),
            spark_max("amount").alias("max_amount"),
            fraud_count_column(),
            fraud_rate_column(),
            high_value_count_column(high_value_threshold),
        )
        .orderBy("event_date")
    )


def build_segment_risk_summary(
    transactions_df: DataFrame,
    high_value_threshold: float,
) -> DataFrame:
    return add_generation_timestamp(
        transactions_df
        .groupBy(
            "country",
            "merchant_category",
            "payment_method",
        )
        .agg(
            spark_count("*").alias("transaction_count"),
            spark_sum("amount").alias("total_amount"),
            spark_avg("amount").alias("avg_amount"),
            spark_max("amount").alias("max_amount"),
            fraud_count_column(),
            fraud_rate_column(),
            high_value_count_column(high_value_threshold),
            countDistinct("customer_id").alias("distinct_customers"),
            countDistinct("merchant_id").alias("distinct_merchants"),
        )
    )


def build_customer_risk_features(
    transactions_df: DataFrame,
    high_value_threshold: float,
) -> DataFrame:
    return add_generation_timestamp(
        transactions_df
        .groupBy("customer_id")
        .agg(
            spark_count("*").alias("transaction_count"),
            spark_sum("amount").alias("total_amount"),
            spark_avg("amount").alias("avg_amount"),
            spark_max("amount").alias("max_amount"),
            fraud_count_column(),
            fraud_rate_column(),
            high_value_count_column(high_value_threshold),
            countDistinct("merchant_id").alias("distinct_merchants"),
            countDistinct("merchant_category").alias("distinct_categories"),
            countDistinct("country").alias("distinct_countries"),
            countDistinct("payment_method").alias("distinct_payment_methods"),
            countDistinct("event_date").alias("active_days"),
            spark_min("event_timestamp").alias("first_seen_at"),
            spark_max("event_timestamp").alias("last_seen_at"),
        )
    )


def build_merchant_risk_features(
    transactions_df: DataFrame,
    high_value_threshold: float,
) -> DataFrame:
    return add_generation_timestamp(
        transactions_df
        .groupBy("merchant_id")
        .agg(
            spark_count("*").alias("transaction_count"),
            spark_sum("amount").alias("total_amount"),
            spark_avg("amount").alias("avg_amount"),
            spark_max("amount").alias("max_amount"),
            fraud_count_column(),
            fraud_rate_column(),
            high_value_count_column(high_value_threshold),
            countDistinct("customer_id").alias("distinct_customers"),
            countDistinct("merchant_category").alias("distinct_categories"),
            countDistinct("country").alias("distinct_countries"),
            countDistinct("payment_method").alias("distinct_payment_methods"),
            countDistinct("event_date").alias("active_days"),
            spark_min("event_timestamp").alias("first_seen_at"),
            spark_max("event_timestamp").alias("last_seen_at"),
        )
    )


def build_high_risk_transactions(
    transactions_df: DataFrame,
    high_value_threshold: float,
) -> DataFrame:
    risk_reason = (
        when(
            col("is_fraud") & (col("amount") >= high_value_threshold),
            "known_fraud_and_high_value",
        )
        .when(
            col("is_fraud"),
            "known_fraud",
        )
        .otherwise("high_value")
    )

    return (
        transactions_df
        .filter(
            col("is_fraud")
            | (col("amount") >= high_value_threshold)
        )
        .withColumn("risk_reason", risk_reason)
        .withColumn("feature_generated_at", current_timestamp())
        .select(
            "transaction_id",
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
            "risk_reason",
            "source_file",
            "processed_at",
            "feature_generated_at",
        )
    )


def write_feature_table(
    features_df: DataFrame,
    output_path: Path,
    partition_by: str | None = None,
) -> int:
    row_count = features_df.count()

    writer = features_df.write.mode("overwrite")

    if partition_by is not None:
        writer = writer.partitionBy(partition_by)

    writer.parquet(str(output_path))

    return row_count


def build_risk_features(
    spark: SparkSession,
    input_path: Path,
    output_root: Path,
    high_value_threshold: float,
) -> None:
    transactions_df = spark.read.parquet(str(input_path))

    feature_tables = [
        (
            "daily_transaction_summary",
            build_daily_transaction_summary(
                transactions_df,
                high_value_threshold,
            ),
            None,
        ),
        (
            "segment_risk_summary",
            build_segment_risk_summary(
                transactions_df,
                high_value_threshold,
            ),
            None,
        ),
        (
            "customer_risk_features",
            build_customer_risk_features(
                transactions_df,
                high_value_threshold,
            ),
            None,
        ),
        (
            "merchant_risk_features",
            build_merchant_risk_features(
                transactions_df,
                high_value_threshold,
            ),
            None,
        ),
        (
            "high_risk_transactions",
            build_high_risk_transactions(
                transactions_df,
                high_value_threshold,
            ),
            "event_date",
        ),
    ]

    print("RISK FEATURE BUILD")
    print("-" * 50)
    print(f"Input path: {input_path}")
    print(f"Output root: {output_root}")
    print(f"High-value threshold: {high_value_threshold:,.2f}")
    print()

    for table_name, table_df, partition_by in feature_tables:
        output_path = output_root / table_name

        row_count = write_feature_table(
            features_df=table_df,
            output_path=output_path,
            partition_by=partition_by,
        )

        print(
            f"Wrote {table_name}: "
            f"{row_count:,} rows -> {output_path}"
        )

    print()
    print("RISK FEATURE BUILD COMPLETE")


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build analytics-ready risk feature tables."
    )

    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/processed/transactions"),
        help="Path to processed transaction Parquet data.",
    )

    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/analytics/risk_features"),
        help="Output root for risk feature Parquet tables.",
    )

    parser.add_argument(
        "--high-value-threshold",
        type=float,
        default=1000.0,
        help="Amount threshold used for high-value risk features.",
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

    if args.high_value_threshold <= 0:
        print("--high-value-threshold must be greater than zero.")
        sys.exit(1)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        build_risk_features(
            spark=spark,
            input_path=args.input,
            output_root=args.output,
            high_value_threshold=args.high_value_threshold,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
