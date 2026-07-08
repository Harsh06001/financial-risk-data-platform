import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    hour,
    input_file_name,
    to_date,
    try_to_timestamp,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)


RAW_TRANSACTION_SCHEMA = StructType(
    [
        StructField("transaction_id", StringType(), nullable=False),
        StructField("event_timestamp", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("merchant_id", StringType(), nullable=False),
        StructField("amount", DoubleType(), nullable=False),
        StructField("currency", StringType(), nullable=False),
        StructField("country", StringType(), nullable=False),
        StructField("merchant_category", StringType(), nullable=False),
        StructField("payment_method", StringType(), nullable=False),
        StructField("device_id", StringType(), nullable=False),
        StructField("is_fraud", BooleanType(), nullable=False),
    ]
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FinancialRiskBatchProcessing")
        .master("local[*]")
        .getOrCreate()
    )


def read_raw_transactions(
    spark: SparkSession,
    input_path: str,
) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        .schema(RAW_TRANSACTION_SCHEMA)
        .csv(input_path)
    )

def transform_transactions(
    transactions_df: DataFrame,
) -> DataFrame:
    return (
        transactions_df
        .withColumn(
            "event_timestamp",
            try_to_timestamp(col("event_timestamp")),
        )
        .withColumn(
            "event_date",
            to_date(col("event_timestamp")),
        )
        .withColumn(
            "event_hour",
            hour(col("event_timestamp")),
        )
        .withColumn(
            "source_file",
            input_file_name(),
        )
        .withColumn(
            "processed_at",
            current_timestamp(),
        )
    )

def clean_transactions(
    transactions_df: DataFrame,
) -> DataFrame:
    return (
        transactions_df
        .filter(col("transaction_id").isNotNull())
        .filter(col("event_timestamp").isNotNull())
        .filter(
            col("amount").isNotNull()
            & (col("amount") > 0)
        )
        .dropDuplicates(["transaction_id"])
    )

def write_processed_transactions(
    transactions_df: DataFrame,
    output_path: str,
    write_strategy: str,
) -> None:
    output_df = transactions_df

    if write_strategy == "repartitioned":
        output_df = transactions_df.repartition("event_date")

    (
        output_df.write
        .mode("overwrite")
        .partitionBy("event_date")
        .parquet(output_path)
    )

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process raw financial transaction data with PySpark."
    )

    parser.add_argument(
        "--input",
        default="data/raw/transactions/*.csv",
        help="Input path or glob for raw transaction CSV files.",
    )
    parser.add_argument(
        "--output",
        default="data/processed/transactions",
        help="Output directory for processed Parquet data.",
    )

    parser.add_argument(
        "--write-strategy",
        choices=["baseline", "repartitioned"],
        default="repartitioned",
        help="Physical write strategy used for processed Parquet output.",
    )

    return parser.parse_args()


def main() -> None:
    args = parse_arguments()

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        transactions_df = read_raw_transactions(
            spark=spark,
            input_path=args.input,
        )
        processed_df = transform_transactions(transactions_df)
        cleaned_df = clean_transactions(processed_df)
        raw_count = transactions_df.count()
        cleaned_count = cleaned_df.count()

        rejected_count = raw_count - cleaned_count

        rejection_rate = (
            rejected_count / raw_count
            if raw_count > 0
            else 0.0
        )

        print()
        print("RAW DATASET SUMMARY")
        print("-" * 50)
        print(f"Input path: {args.input}")
        print(f"Total rows: {raw_count:,}")

        print()
        print("RAW SCHEMA")
        print("-" * 50)
        transactions_df.printSchema()

        print()
        print("SAMPLE ROWS")
        print("-" * 50)
        transactions_df.show(5, truncate=False)

        print()
        print("PROCESSED SCHEMA")
        print("-" * 50)
        processed_df.printSchema()

        print()
        print("PROCESSED SAMPLE")
        print("-" * 50)
        processed_df.select(
            "transaction_id",
            "event_timestamp",
            "event_date",
            "event_hour",
            "amount",
            "source_file",
            "processed_at",
        ).show(5, truncate=False)

        print()
        print("CLEANING METRICS")
        print("-" * 50)
        print(f"Raw rows: {raw_count:,}")
        print(f"Clean rows: {cleaned_count:,}")
        print(f"Rejected rows: {rejected_count:,}")
        print(f"Rejection rate: {rejection_rate:.2%}")
        print()
        print("WRITING PROCESSED DATA")
        print("-" * 50)

        print(f"Write strategy: {args.write_strategy}")
        
        write_processed_transactions(
            transactions_df=cleaned_df,
            output_path=args.output,
            write_strategy=args.write_strategy,
        )

        print(f"Output path: {args.output}")
        print("Write status: COMPLETE")

        reloaded_df = spark.read.parquet(args.output)
        reloaded_count = reloaded_df.count()

        print()
        print("OUTPUT VERIFICATION")
        print("-" * 50)
        print(f"Expected rows: {cleaned_count:,}")
        print(f"Reloaded rows: {reloaded_count:,}")

        if reloaded_count != cleaned_count:
            raise RuntimeError(
                "Processed output row count does not match cleaned row count."
            )

        print("Row count check: PASSED")

        print()
        print("PARQUET SCHEMA")
        print("-" * 50)
        reloaded_df.printSchema()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()