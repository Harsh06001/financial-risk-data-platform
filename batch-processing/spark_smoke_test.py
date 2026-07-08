from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def main() -> None:
    spark = (
        SparkSession.builder
        .appName("FinancialRiskSparkSmokeTest")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    transactions = [
        ("TXN_001", 25.50),
        ("TXN_002", 250.00),
        ("TXN_003", 1500.00),
        ("TXN_004", 75.25),
    ]

    transactions_df = spark.createDataFrame(
        transactions,
        ["transaction_id", "amount"],
    )

    high_value_df = transactions_df.filter(
        col("amount") > 100
    )

    print(f"Spark version: {spark.version}")
    print(f"Input rows: {transactions_df.count()}")
    print(f"High-value rows: {high_value_df.count()}")

    high_value_df.show()

    spark.stop()


if __name__ == "__main__":
    main()