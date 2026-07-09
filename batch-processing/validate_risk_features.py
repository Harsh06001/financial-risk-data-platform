import argparse
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    col,
    sum as spark_sum,
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ValidateRiskFeatures")
        .master("local[*]")
        .getOrCreate()
    )


def validate_customer_risk_features(
    spark: SparkSession,
    input_root: Path,
    expected_customers: int | None,
) -> bool:
    customer_path = input_root / "customer_risk_features"

    print("CUSTOMER RISK FEATURE VALIDATION")
    print("-" * 50)
    print(f"Input path: {customer_path}")
    print()

    if not customer_path.exists():
        print("FAILED: customer_risk_features does not exist.")
        return False

    customer_df = spark.read.parquet(str(customer_path))

    validation_passed = True

    # Check 1: total row count
    row_count = customer_df.count()

    print(f"Customer rows: {row_count:,}")

    if (
        expected_customers is not None
        and row_count != expected_customers
    ):
        print(
            f"FAILED: Expected {expected_customers:,} customers, "
            f"but found {row_count:,}."
        )
        validation_passed = False

    # Check 2: customer_id must not be null
    null_customer_ids = (
        customer_df
        .filter(col("customer_id").isNull())
        .count()
    )

    print(f"Null customer IDs: {null_customer_ids:,}")

    if null_customer_ids > 0:
        print("FAILED: customer_id contains null values.")
        validation_passed = False

    # Check 3: one row per customer
    unique_customers = (
        customer_df
        .select("customer_id")
        .distinct()
        .count()
    )

    print(f"Unique customers: {unique_customers:,}")

    if unique_customers != row_count:
        print(
            "FAILED: customer_risk_features contains "
            "duplicate customer rows."
        )
        validation_passed = False

    # Check 4: every customer must have at least one transaction
    invalid_transaction_counts = (
        customer_df
        .filter(col("transaction_count") <= 0)
        .count()
    )

    print(
        "Invalid transaction counts: "
        f"{invalid_transaction_counts:,}"
    )

    if invalid_transaction_counts > 0:
        print(
            "FAILED: Some customers have "
            "transaction_count <= 0."
        )
        validation_passed = False

    # Check 5: fraud_count must be logically valid
    invalid_fraud_counts = (
        customer_df
        .filter(
            (col("fraud_count") < 0)
            | (
                col("fraud_count")
                > col("transaction_count")
            )
        )
        .count()
    )

    print(
        f"Invalid fraud counts: {invalid_fraud_counts:,}"
    )

    if invalid_fraud_counts > 0:
        print(
            "FAILED: fraud_count must be between "
            "0 and transaction_count."
        )
        validation_passed = False

        # Check 6: fraud_rate must be between 0 and 1
    invalid_fraud_rates = (
        customer_df
        .filter(
            col("fraud_rate").isNull()
            | (col("fraud_rate") < 0)
            | (col("fraud_rate") > 1)
        )
        .count()
    )

    print(
        f"Invalid fraud rates: {invalid_fraud_rates:,}"
    )

    if invalid_fraud_rates > 0:
        print(
            "FAILED: fraud_rate must be between 0 and 1."
        )
        validation_passed = False



        # Check 7: fraud_rate must match
    # fraud_count / transaction_count
    fraud_rate_tolerance = 0.000001

    inconsistent_fraud_rates = (
        customer_df
        .filter(
            spark_abs(
                col("fraud_rate")
                - (
                    col("fraud_count")
                    / col("transaction_count")
                )
            )
            > fraud_rate_tolerance
        )
        .count()
    )

    print(
        "Inconsistent fraud rates: "
        f"{inconsistent_fraud_rates:,}"
    )

    if inconsistent_fraud_rates > 0:
        print(
            "FAILED: fraud_rate does not match "
            "fraud_count / transaction_count."
        )
        validation_passed = False

        # Check 8: high-value transaction count must be valid
    invalid_high_value_counts = (
        customer_df
        .filter(
            col("high_value_transaction_count").isNull()
            | (col("high_value_transaction_count") < 0)
            | (
                col("high_value_transaction_count")
                > col("transaction_count")
            )
        )
        .count()
    )

    print(
        "Invalid high-value transaction counts: "
        f"{invalid_high_value_counts:,}"
    )

    if invalid_high_value_counts > 0:
        print(
            "FAILED: high_value_transaction_count must be "
            "between 0 and transaction_count."
        )
        validation_passed = False

        # Check 9: active_days must be logically valid
    invalid_active_days = (
        customer_df
        .filter(
            col("active_days").isNull()
            | (col("active_days") <= 0)
            | (
                col("active_days")
                > col("transaction_count")
            )
        )
        .count()
    )

    print(
        f"Invalid active days: {invalid_active_days:,}"
    )

    if invalid_active_days > 0:
        print(
            "FAILED: active_days must be between "
            "1 and transaction_count."
        )
        validation_passed = False
        # Check 10: customer activity window must be valid
    invalid_activity_windows = (
        customer_df
        .filter(
            col("first_seen_at").isNull()
            | col("last_seen_at").isNull()
            | (
                col("first_seen_at")
                > col("last_seen_at")
            )
        )
        .count()
    )

    print(
        "Invalid activity windows: "
        f"{invalid_activity_windows:,}"
    )

    if invalid_activity_windows > 0:
        print(
            "FAILED: first_seen_at must be less than "
            "or equal to last_seen_at."
        )
        validation_passed = False


    print()

    if validation_passed:
        print("CUSTOMER RISK FEATURE VALIDATION PASSED")
    else:
        print("CUSTOMER RISK FEATURE VALIDATION FAILED")

    return validation_passed
def validate_merchant_risk_features(
    spark: SparkSession,
    input_root: Path,
    expected_merchants: int | None,
) -> bool:
    merchant_path = input_root / "merchant_risk_features"

    print("MERCHANT RISK FEATURE VALIDATION")
    print("-" * 50)
    print(f"Input path: {merchant_path}")
    print()

    if not merchant_path.exists():
        print("FAILED: merchant_risk_features does not exist.")
        return False

    merchant_df = spark.read.parquet(str(merchant_path))

    validation_passed = True

    # Check 1: total row count
    row_count = merchant_df.count()

    print(f"Merchant rows: {row_count:,}")

    if (
        expected_merchants is not None
        and row_count != expected_merchants
    ):
        print(
            f"FAILED: Expected {expected_merchants:,} merchants, "
            f"but found {row_count:,}."
        )
        validation_passed = False

    # Check 2: merchant_id must not be null
    null_merchant_ids = (
        merchant_df
        .filter(col("merchant_id").isNull())
        .count()
    )

    print(f"Null merchant IDs: {null_merchant_ids:,}")

    if null_merchant_ids > 0:
        print("FAILED: merchant_id contains null values.")
        validation_passed = False

    # Check 3: one row per merchant
    unique_merchants = (
        merchant_df
        .select("merchant_id")
        .distinct()
        .count()
    )

    print(f"Unique merchants: {unique_merchants:,}")

    if unique_merchants != row_count:
        print(
            "FAILED: merchant_risk_features contains "
            "duplicate merchant rows."
        )
        validation_passed = False

    # Check 4: every merchant must have at least one transaction
    invalid_transaction_counts = (
        merchant_df
        .filter(
            col("transaction_count").isNull()
            | (col("transaction_count") <= 0)
        )
        .count()
    )

    print(
        "Invalid transaction counts: "
        f"{invalid_transaction_counts:,}"
    )

    if invalid_transaction_counts > 0:
        print(
            "FAILED: Some merchants have "
            "transaction_count <= 0 or null."
        )
        validation_passed = False

    # Check 5: fraud_count must be logically valid
    invalid_fraud_counts = (
        merchant_df
        .filter(
            col("fraud_count").isNull()
            | (col("fraud_count") < 0)
            | (
                col("fraud_count")
                > col("transaction_count")
            )
        )
        .count()
    )

    print(
        f"Invalid fraud counts: {invalid_fraud_counts:,}"
    )

    if invalid_fraud_counts > 0:
        print(
            "FAILED: fraud_count must be between "
            "0 and transaction_count."
        )
        validation_passed = False

    # Check 6: fraud_rate must be between 0 and 1
    invalid_fraud_rates = (
        merchant_df
        .filter(
            col("fraud_rate").isNull()
            | (col("fraud_rate") < 0)
            | (col("fraud_rate") > 1)
        )
        .count()
    )

    print(
        f"Invalid fraud rates: {invalid_fraud_rates:,}"
    )

    if invalid_fraud_rates > 0:
        print(
            "FAILED: fraud_rate must be between 0 and 1."
        )
        validation_passed = False

    # Check 7: fraud_rate must match
    # fraud_count / transaction_count
    fraud_rate_tolerance = 0.000001

    inconsistent_fraud_rates = (
        merchant_df
        .filter(
            spark_abs(
                col("fraud_rate")
                - (
                    col("fraud_count")
                    / col("transaction_count")
                )
            )
            > fraud_rate_tolerance
        )
        .count()
    )

    print(
        "Inconsistent fraud rates: "
        f"{inconsistent_fraud_rates:,}"
    )

    if inconsistent_fraud_rates > 0:
        print(
            "FAILED: fraud_rate does not match "
            "fraud_count / transaction_count."
        )
        validation_passed = False

    # Check 8: high-value transaction count must be valid
    invalid_high_value_counts = (
        merchant_df
        .filter(
            col("high_value_transaction_count").isNull()
            | (col("high_value_transaction_count") < 0)
            | (
                col("high_value_transaction_count")
                > col("transaction_count")
            )
        )
        .count()
    )

    print(
        "Invalid high-value transaction counts: "
        f"{invalid_high_value_counts:,}"
    )

    if invalid_high_value_counts > 0:
        print(
            "FAILED: high_value_transaction_count must be "
            "between 0 and transaction_count."
        )
        validation_passed = False

    # Check 9: active_days must be logically valid
    invalid_active_days = (
        merchant_df
        .filter(
            col("active_days").isNull()
            | (col("active_days") <= 0)
            | (
                col("active_days")
                > col("transaction_count")
            )
        )
        .count()
    )

    print(
        f"Invalid active days: {invalid_active_days:,}"
    )

    if invalid_active_days > 0:
        print(
            "FAILED: active_days must be between "
            "1 and transaction_count."
        )
        validation_passed = False

    # Check 10: merchant activity window must be valid
    invalid_activity_windows = (
        merchant_df
        .filter(
            col("first_seen_at").isNull()
            | col("last_seen_at").isNull()
            | (
                col("first_seen_at")
                > col("last_seen_at")
            )
        )
        .count()
    )

    print(
        "Invalid activity windows: "
        f"{invalid_activity_windows:,}"
    )

    if invalid_activity_windows > 0:
        print(
            "FAILED: first_seen_at must be less than "
            "or equal to last_seen_at."
        )
        validation_passed = False

    print()

    if validation_passed:
        print("MERCHANT RISK FEATURE VALIDATION PASSED")
    else:
        print("MERCHANT RISK FEATURE VALIDATION FAILED")

    return validation_passed

def validate_daily_transaction_summary(
    spark: SparkSession,
    input_root: Path,
    expected_event_dates: int | None,
    expected_transactions: int | None,
) -> bool:
    daily_path = input_root / "daily_transaction_summary"

    print("DAILY TRANSACTION SUMMARY VALIDATION")
    print("-" * 50)
    print(f"Input path: {daily_path}")
    print()

    if not daily_path.exists():
        print("FAILED: daily_transaction_summary does not exist.")
        return False

    daily_df = spark.read.parquet(str(daily_path))

    validation_passed = True

    # Check 1: expected number of daily rows
    row_count = daily_df.count()

    print(f"Daily summary rows: {row_count:,}")

    if (
        expected_event_dates is not None
        and row_count != expected_event_dates
    ):
        print(
            f"FAILED: Expected {expected_event_dates:,} event dates, "
            f"but found {row_count:,}."
        )
        validation_passed = False

    # Check 2: event_date must not be null
    null_event_dates = (
        daily_df
        .filter(col("event_date").isNull())
        .count()
    )

    print(f"Null event dates: {null_event_dates:,}")

    if null_event_dates > 0:
        print("FAILED: event_date contains null values.")
        validation_passed = False

    # Check 3: one row per event_date
    unique_event_dates = (
        daily_df
        .select("event_date")
        .distinct()
        .count()
    )

    print(f"Unique event dates: {unique_event_dates:,}")

    if unique_event_dates != row_count:
        print(
            "FAILED: daily_transaction_summary contains "
            "duplicate event_date rows."
        )
        validation_passed = False

    # Check 4: every date must contain transactions
    invalid_transaction_counts = (
        daily_df
        .filter(
            col("transaction_count").isNull()
            | (col("transaction_count") <= 0)
        )
        .count()
    )

    print(
        "Invalid transaction counts: "
        f"{invalid_transaction_counts:,}"
    )

    if invalid_transaction_counts > 0:
        print(
            "FAILED: transaction_count must be greater than zero."
        )
        validation_passed = False

    # Check 5: fraud_count must be valid
    invalid_fraud_counts = (
        daily_df
        .filter(
            col("fraud_count").isNull()
            | (col("fraud_count") < 0)
            | (
                col("fraud_count")
                > col("transaction_count")
            )
        )
        .count()
    )

    print(f"Invalid fraud counts: {invalid_fraud_counts:,}")

    if invalid_fraud_counts > 0:
        print(
            "FAILED: fraud_count must be between "
            "0 and transaction_count."
        )
        validation_passed = False

    # Check 6: fraud_rate must be between 0 and 1
    invalid_fraud_rates = (
        daily_df
        .filter(
            col("fraud_rate").isNull()
            | (col("fraud_rate") < 0)
            | (col("fraud_rate") > 1)
        )
        .count()
    )

    print(f"Invalid fraud rates: {invalid_fraud_rates:,}")

    if invalid_fraud_rates > 0:
        print(
            "FAILED: fraud_rate must be between 0 and 1."
        )
        validation_passed = False

    # Check 7: fraud_rate must match
    # fraud_count / transaction_count
    fraud_rate_tolerance = 0.000001

    inconsistent_fraud_rates = (
        daily_df
        .filter(
            spark_abs(
                col("fraud_rate")
                - (
                    col("fraud_count")
                    / col("transaction_count")
                )
            )
            > fraud_rate_tolerance
        )
        .count()
    )

    print(
        "Inconsistent fraud rates: "
        f"{inconsistent_fraud_rates:,}"
    )

    if inconsistent_fraud_rates > 0:
        print(
            "FAILED: fraud_rate does not match "
            "fraud_count / transaction_count."
        )
        validation_passed = False

    # Check 8: high-value transaction count must be valid
    invalid_high_value_counts = (
        daily_df
        .filter(
            col("high_value_transaction_count").isNull()
            | (col("high_value_transaction_count") < 0)
            | (
                col("high_value_transaction_count")
                > col("transaction_count")
            )
        )
        .count()
    )

    print(
        "Invalid high-value transaction counts: "
        f"{invalid_high_value_counts:,}"
    )

    if invalid_high_value_counts > 0:
        print(
            "FAILED: high_value_transaction_count must be "
            "between 0 and transaction_count."
        )
        validation_passed = False

    # Check 9: amount statistics must be logically ordered
    invalid_amount_statistics = (
        daily_df
        .filter(
            col("min_amount").isNull()
            | col("avg_amount").isNull()
            | col("max_amount").isNull()
            | (col("min_amount") <= 0)
            | (col("min_amount") > col("avg_amount"))
            | (col("avg_amount") > col("max_amount"))
        )
        .count()
    )

    print(
        "Invalid amount statistics: "
        f"{invalid_amount_statistics:,}"
    )

    if invalid_amount_statistics > 0:
        print(
            "FAILED: Amount statistics must satisfy "
            "0 < min_amount <= avg_amount <= max_amount."
        )
        validation_passed = False

    # Check 10: daily transaction counts must reconcile
    # with the processed source total
    total_transactions = (
        daily_df
        .agg(
            spark_sum("transaction_count")
            .alias("total_transactions")
        )
        .first()["total_transactions"]
    )

    print(
        "Reconciled transaction total: "
        f"{total_transactions:,}"
    )

    if (
        expected_transactions is not None
        and total_transactions != expected_transactions
    ):
        print(
            f"FAILED: Expected {expected_transactions:,} total "
            f"transactions, but daily summaries contain "
            f"{total_transactions:,}."
        )
        validation_passed = False

    print()

    if validation_passed:
        print("DAILY TRANSACTION SUMMARY VALIDATION PASSED")
    else:
        print("DAILY TRANSACTION SUMMARY VALIDATION FAILED")

    return validation_passed

def validate_segment_risk_summary(
    spark: SparkSession,
    input_root: Path,
    expected_segments: int | None,
    expected_transactions: int | None,
) -> bool:
    segment_path = input_root / "segment_risk_summary"

    print("SEGMENT RISK SUMMARY VALIDATION")
    print("-" * 50)
    print(f"Input path: {segment_path}")
    print()

    if not segment_path.exists():
        print("FAILED: segment_risk_summary does not exist.")
        return False

    segment_df = spark.read.parquet(str(segment_path))

    validation_passed = True

    # Check 1: expected number of segment rows
    row_count = segment_df.count()

    print(f"Segment rows: {row_count:,}")

    if (
        expected_segments is not None
        and row_count != expected_segments
    ):
        print(
            f"FAILED: Expected {expected_segments:,} segments, "
            f"but found {row_count:,}."
        )
        validation_passed = False

    # Check 2: segment key columns must not be null
    null_segment_keys = (
        segment_df
        .filter(
            col("country").isNull()
            | col("merchant_category").isNull()
            | col("payment_method").isNull()
        )
        .count()
    )

    print(f"Null segment keys: {null_segment_keys:,}")

    if null_segment_keys > 0:
        print(
            "FAILED: Segment key columns contain null values."
        )
        validation_passed = False

    # Check 3: one row per unique segment combination
    unique_segments = (
        segment_df
        .select(
            "country",
            "merchant_category",
            "payment_method",
        )
        .distinct()
        .count()
    )

    print(f"Unique segments: {unique_segments:,}")

    if unique_segments != row_count:
        print(
            "FAILED: segment_risk_summary contains "
            "duplicate segment combinations."
        )
        validation_passed = False

    # Check 4: every segment must contain transactions
    invalid_transaction_counts = (
        segment_df
        .filter(
            col("transaction_count").isNull()
            | (col("transaction_count") <= 0)
        )
        .count()
    )

    print(
        "Invalid transaction counts: "
        f"{invalid_transaction_counts:,}"
    )

    if invalid_transaction_counts > 0:
        print(
            "FAILED: transaction_count must be greater than zero."
        )
        validation_passed = False

    # Check 5: fraud_count must be logically valid
    invalid_fraud_counts = (
        segment_df
        .filter(
            col("fraud_count").isNull()
            | (col("fraud_count") < 0)
            | (
                col("fraud_count")
                > col("transaction_count")
            )
        )
        .count()
    )

    print(f"Invalid fraud counts: {invalid_fraud_counts:,}")

    if invalid_fraud_counts > 0:
        print(
            "FAILED: fraud_count must be between "
            "0 and transaction_count."
        )
        validation_passed = False

    # Check 6: fraud_rate must be between 0 and 1
    invalid_fraud_rates = (
        segment_df
        .filter(
            col("fraud_rate").isNull()
            | (col("fraud_rate") < 0)
            | (col("fraud_rate") > 1)
        )
        .count()
    )

    print(f"Invalid fraud rates: {invalid_fraud_rates:,}")

    if invalid_fraud_rates > 0:
        print(
            "FAILED: fraud_rate must be between 0 and 1."
        )
        validation_passed = False

    # Check 7: fraud_rate must match
    # fraud_count / transaction_count
    fraud_rate_tolerance = 0.000001

    inconsistent_fraud_rates = (
        segment_df
        .filter(
            spark_abs(
                col("fraud_rate")
                - (
                    col("fraud_count")
                    / col("transaction_count")
                )
            )
            > fraud_rate_tolerance
        )
        .count()
    )

    print(
        "Inconsistent fraud rates: "
        f"{inconsistent_fraud_rates:,}"
    )

    if inconsistent_fraud_rates > 0:
        print(
            "FAILED: fraud_rate does not match "
            "fraud_count / transaction_count."
        )
        validation_passed = False

    # Check 8: high-value transaction count must be valid
    invalid_high_value_counts = (
        segment_df
        .filter(
            col("high_value_transaction_count").isNull()
            | (col("high_value_transaction_count") < 0)
            | (
                col("high_value_transaction_count")
                > col("transaction_count")
            )
        )
        .count()
    )

    print(
        "Invalid high-value transaction counts: "
        f"{invalid_high_value_counts:,}"
    )

    if invalid_high_value_counts > 0:
        print(
            "FAILED: high_value_transaction_count must be "
            "between 0 and transaction_count."
        )
        validation_passed = False

    # Check 9: distinct entity counts must be valid
    invalid_distinct_counts = (
        segment_df
        .filter(
            col("distinct_customers").isNull()
            | col("distinct_merchants").isNull()
            | (col("distinct_customers") <= 0)
            | (col("distinct_merchants") <= 0)
            | (
                col("distinct_customers")
                > col("transaction_count")
            )
            | (
                col("distinct_merchants")
                > col("transaction_count")
            )
        )
        .count()
    )

    print(
        "Invalid distinct entity counts: "
        f"{invalid_distinct_counts:,}"
    )

    if invalid_distinct_counts > 0:
        print(
            "FAILED: distinct customer and merchant counts "
            "must be between 1 and transaction_count."
        )
        validation_passed = False

    # Check 10: segment transaction counts must reconcile
    total_transactions = (
        segment_df
        .agg(
            spark_sum("transaction_count")
            .alias("total_transactions")
        )
        .first()["total_transactions"]
    )

    print(
        "Reconciled transaction total: "
        f"{total_transactions:,}"
    )

    if (
        expected_transactions is not None
        and total_transactions != expected_transactions
    ):
        print(
            f"FAILED: Expected {expected_transactions:,} total "
            f"transactions, but segment summaries contain "
            f"{total_transactions:,}."
        )
        validation_passed = False

    print()

    if validation_passed:
        print("SEGMENT RISK SUMMARY VALIDATION PASSED")
    else:
        print("SEGMENT RISK SUMMARY VALIDATION FAILED")

    return validation_passed
def validate_high_risk_transactions(
    spark: SparkSession,
    input_root: Path,
    high_value_threshold: float,
    expected_high_risk_transactions: int | None,
) -> bool:
    high_risk_path = input_root / "high_risk_transactions"

    print("HIGH-RISK TRANSACTION VALIDATION")
    print("-" * 50)
    print(f"Input path: {high_risk_path}")
    print(f"High-value threshold: {high_value_threshold:,.2f}")
    print()

    if not high_risk_path.exists():
        print("FAILED: high_risk_transactions does not exist.")
        return False

    high_risk_df = spark.read.parquet(str(high_risk_path))

    validation_passed = True

    # Check 1: expected row count
    row_count = high_risk_df.count()

    print(f"High-risk transaction rows: {row_count:,}")

    if (
        expected_high_risk_transactions is not None
        and row_count != expected_high_risk_transactions
    ):
        print(
            f"FAILED: Expected "
            f"{expected_high_risk_transactions:,} high-risk transactions, "
            f"but found {row_count:,}."
        )
        validation_passed = False

    # Check 2: transaction_id must not be null
    null_transaction_ids = (
        high_risk_df
        .filter(col("transaction_id").isNull())
        .count()
    )

    print(f"Null transaction IDs: {null_transaction_ids:,}")

    if null_transaction_ids > 0:
        print("FAILED: transaction_id contains null values.")
        validation_passed = False

    # Check 3: one row per transaction
    unique_transactions = (
        high_risk_df
        .select("transaction_id")
        .distinct()
        .count()
    )

    print(f"Unique transactions: {unique_transactions:,}")

    if unique_transactions != row_count:
        print(
            "FAILED: high_risk_transactions contains "
            "duplicate transaction rows."
        )
        validation_passed = False

    # Check 4: every row must satisfy the high-risk filter
    invalid_high_risk_rows = (
        high_risk_df
        .filter(
            col("is_fraud").isNull()
            | col("amount").isNull()
            | (
                (~col("is_fraud"))
                & (col("amount") < high_value_threshold)
            )
        )
        .count()
    )

    print(
        "Rows that do not satisfy high-risk rule: "
        f"{invalid_high_risk_rows:,}"
    )

    if invalid_high_risk_rows > 0:
        print(
            "FAILED: Every row must be fraudulent "
            "or meet the high-value threshold."
        )
        validation_passed = False

    # Check 5: risk_reason must contain only expected values
    invalid_risk_reasons = (
        high_risk_df
        .filter(
            col("risk_reason").isNull()
            | (
                ~col("risk_reason").isin(
                    "known_fraud_and_high_value",
                    "known_fraud",
                    "high_value",
                )
            )
        )
        .count()
    )

    print(
        f"Invalid risk reasons: {invalid_risk_reasons:,}"
    )

    if invalid_risk_reasons > 0:
        print(
            "FAILED: risk_reason contains unexpected values."
        )
        validation_passed = False

    # Check 6: known_fraud_and_high_value must match the data
    invalid_fraud_and_high_value_reasons = (
        high_risk_df
        .filter(
            (col("risk_reason") == "known_fraud_and_high_value")
            & (
                (~col("is_fraud"))
                | (col("amount") < high_value_threshold)
            )
        )
        .count()
    )

    print(
        "Invalid fraud-and-high-value reasons: "
        f"{invalid_fraud_and_high_value_reasons:,}"
    )

    if invalid_fraud_and_high_value_reasons > 0:
        print(
            "FAILED: known_fraud_and_high_value rows must "
            "be fraudulent and high value."
        )
        validation_passed = False

    # Check 7: known_fraud must match the data
    invalid_known_fraud_reasons = (
        high_risk_df
        .filter(
            (col("risk_reason") == "known_fraud")
            & (
                (~col("is_fraud"))
                | (col("amount") >= high_value_threshold)
            )
        )
        .count()
    )

    print(
        "Invalid known-fraud reasons: "
        f"{invalid_known_fraud_reasons:,}"
    )

    if invalid_known_fraud_reasons > 0:
        print(
            "FAILED: known_fraud rows must be fraudulent "
            "and below the high-value threshold."
        )
        validation_passed = False

    # Check 8: high_value must match the data
    invalid_high_value_reasons = (
        high_risk_df
        .filter(
            (col("risk_reason") == "high_value")
            & (
                col("is_fraud")
                | (col("amount") < high_value_threshold)
            )
        )
        .count()
    )

    print(
        "Invalid high-value reasons: "
        f"{invalid_high_value_reasons:,}"
    )

    if invalid_high_value_reasons > 0:
        print(
            "FAILED: high_value rows must be non-fraudulent "
            "and meet the threshold."
        )
        validation_passed = False

    # Check 9: event_date must not be null
    null_event_dates = (
        high_risk_df
        .filter(col("event_date").isNull())
        .count()
    )

    print(f"Null event dates: {null_event_dates:,}")

    if null_event_dates > 0:
        print("FAILED: event_date contains null values.")
        validation_passed = False

    print()

    if validation_passed:
        print("HIGH-RISK TRANSACTION VALIDATION PASSED")
    else:
        print("HIGH-RISK TRANSACTION VALIDATION FAILED")

    return validation_passed

def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate generated risk feature tables."
    )

    parser.add_argument(
        "--input",
        type=Path,
        default=Path("data/analytics/risk_features"),
        help="Root directory containing risk feature tables.",
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
        "--expected-event-dates",
        type=int,
        default=None,
        help="Optional expected number of daily summary rows.",
    )

    parser.add_argument(
        "--expected-transactions",
        type=int,
        default=None,
        help="Optional expected total transaction count.",
    )

    parser.add_argument(
        "--expected-segments",
        type=int,
        default=None,
        help="Optional expected number of segment summary rows.",
    )

    parser.add_argument(
        "--high-value-threshold",
        type=float,
        default=1000.0,
        help="Amount threshold used to validate high-risk transactions.",
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

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        customer_validation_passed = (
            validate_customer_risk_features(
                spark=spark,
                input_root=args.input,
                expected_customers=args.expected_customers,
            )
        )

        print()

        merchant_validation_passed = (
            validate_merchant_risk_features(
                spark=spark,
                input_root=args.input,
                expected_merchants=args.expected_merchants,
            )
        )

        print()

        daily_validation_passed = (
            validate_daily_transaction_summary(
                spark=spark,
                input_root=args.input,
                expected_event_dates=args.expected_event_dates,
                expected_transactions=args.expected_transactions,
            )
        )

        print()

        segment_validation_passed = (
            validate_segment_risk_summary(
                spark=spark,
                input_root=args.input,
                expected_segments=args.expected_segments,
                expected_transactions=args.expected_transactions,
            )
        )

        print()

        high_risk_validation_passed = (
            validate_high_risk_transactions(
                spark=spark,
                input_root=args.input,
                high_value_threshold=args.high_value_threshold,
                expected_high_risk_transactions=(
                    args.expected_high_risk_transactions
                ),
            )
        )

    finally:
        spark.stop()

    validation_passed = (
        customer_validation_passed
        and merchant_validation_passed
        and daily_validation_passed
        and segment_validation_passed
        and high_risk_validation_passed
    )

    if not validation_passed:
        sys.exit(1)

if __name__ == "__main__":
    main()