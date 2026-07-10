import json
import subprocess
import sys
from pathlib import Path


PROJECT_ID = "risk-data-platform-npg-2026"
LOCATION = "us-central1"
DATASET_ID = "risk_analytics"


class ValidationError(RuntimeError):
    def __init__(self, table: str, contract: str, expected: object, actual: object) -> None:
        super().__init__(
            f"{table}: {contract} failed. Expected {expected!r}, got {actual!r}"
        )
        self.table = table
        self.contract = contract
        self.expected = expected
        self.actual = actual


def run_query(query: str) -> list[dict]:
    command = [
        "bq",
        f"--project_id={PROJECT_ID}",
        f"--location={LOCATION}",
        "query",
        "--format=json",
        "--use_legacy_sql=false",
        query,
    ]

    completed = subprocess.run(command, capture_output=True, text=True, check=True)

    try:
        return json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Unable to parse BigQuery JSON output: {completed.stdout}") from exc


def require(condition: bool, table: str, contract: str, expected: object, actual: object) -> None:
    if not condition:
        raise ValidationError(table, contract, expected, actual)


def validate_processed_transactions() -> None:
    table = "processed_transactions"
    query = f"""
    SELECT
      COUNT(*) AS row_count,
      COUNT(DISTINCT transaction_id) AS unique_transaction_ids,
      COUNTIF(transaction_id IS NULL) AS null_transaction_ids,
      COUNTIF(event_timestamp IS NULL) AS null_event_timestamps,
      COUNTIF(event_date IS NULL) AS null_event_dates,
      COUNTIF(customer_id IS NULL) AS null_customer_ids,
      COUNTIF(merchant_id IS NULL) AS null_merchant_ids,
      COUNTIF(amount IS NULL OR amount <= 0) AS invalid_amount_rows,
      COUNTIF(event_hour IS NULL OR event_hour < 0 OR event_hour > 23) AS invalid_event_hour_rows,
      COUNTIF(is_fraud) AS fraud_rows,
      COUNT(DISTINCT event_date) AS distinct_event_dates,
      MIN(event_date) AS min_event_date,
      MAX(event_date) AS max_event_date
    FROM `{PROJECT_ID}.{DATASET_ID}.{table}`
    """
    row = run_query(query)[0]

    require(int(row["row_count"]) == 100350, table, "row count", 100350, int(row["row_count"]))
    require(int(row["unique_transaction_ids"]) == 100350, table, "unique transaction IDs", 100350, int(row["unique_transaction_ids"]))
    require(int(row["null_transaction_ids"]) == 0, table, "null transaction IDs", 0, int(row["null_transaction_ids"]))
    require(int(row["null_event_timestamps"]) == 0, table, "null event timestamps", 0, int(row["null_event_timestamps"]))
    require(int(row["null_event_dates"]) == 0, table, "null event dates", 0, int(row["null_event_dates"]))
    require(int(row["null_customer_ids"]) == 0, table, "null customer IDs", 0, int(row["null_customer_ids"]))
    require(int(row["null_merchant_ids"]) == 0, table, "null merchant IDs", 0, int(row["null_merchant_ids"]))
    require(int(row["invalid_amount_rows"]) == 0, table, "positive amounts", 0, int(row["invalid_amount_rows"]))
    require(int(row["invalid_event_hour_rows"]) == 0, table, "event hour range", 0, int(row["invalid_event_hour_rows"]))
    require(int(row["fraud_rows"]) == 3261, table, "fraud row count", 3261, int(row["fraud_rows"]))
    require(int(row["distinct_event_dates"]) == 31, table, "distinct event dates", 31, int(row["distinct_event_dates"]))
    require(row["min_event_date"] == "2026-06-08", table, "minimum event date", "2026-06-08", row["min_event_date"])
    require(row["max_event_date"] == "2026-07-08", table, "maximum event date", "2026-07-08", row["max_event_date"])

    partition_query = f"""
    SELECT COUNT(*) AS partition_count
    FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.PARTITIONS`
    WHERE table_name = '{table}'
    """
    partition_count = int(run_query(partition_query)[0]["partition_count"])
    require(partition_count == 31, table, "partition count", 31, partition_count)
    print(
        f"Validated {table}: row count={row['row_count']}, "
        f"unique transactions={row['unique_transaction_ids']}, "
        f"partitions={partition_count}"
    )


def validate_daily_transaction_summary() -> None:
    table = "daily_transaction_summary"
    query = f"""
    SELECT
      COUNT(*) AS row_count,
      COUNT(DISTINCT event_date) AS distinct_event_dates,
      SUM(transaction_count) AS total_transaction_count,
      SUM(CASE WHEN event_date IS NULL THEN 1 ELSE 0 END) AS null_event_dates,
      SUM(CASE WHEN fraud_count < 0 OR fraud_count > transaction_count THEN 1 ELSE 0 END) AS invalid_fraud_count_rows,
      SUM(CASE WHEN fraud_rate IS NULL OR fraud_rate < 0 OR fraud_rate > 1 THEN 1 ELSE 0 END) AS invalid_fraud_rate_rows,
      SUM(CASE WHEN ABS(fraud_rate - SAFE_DIVIDE(fraud_count, transaction_count)) > 0.000001 THEN 1 ELSE 0 END) AS inconsistent_fraud_rate_rows,
      SUM(CASE WHEN high_value_transaction_count IS NULL OR high_value_transaction_count < 0 OR high_value_transaction_count > transaction_count THEN 1 ELSE 0 END) AS invalid_high_value_count_rows,
      SUM(CASE WHEN min_amount IS NULL OR min_amount <= 0 THEN 1 ELSE 0 END) AS invalid_min_amount_rows,
      SUM(CASE WHEN min_amount > avg_amount OR avg_amount > max_amount THEN 1 ELSE 0 END) AS invalid_amount_order_rows,
      MIN(min_amount) AS min_amount,
      MAX(max_amount) AS max_amount,
      MIN(avg_amount) AS avg_amount
    FROM `{PROJECT_ID}.{DATASET_ID}.{table}`
    """
    rows = run_query(query)
    row = rows[0]
    require(int(row["row_count"]) == 31, table, "row count", 31, int(row["row_count"]))
    require(int(row["distinct_event_dates"]) == 31, table, "distinct event dates", 31, int(row["distinct_event_dates"]))
    require(int(row["total_transaction_count"]) == 100350, table, "SUM(transaction_count)", 100350, int(row["total_transaction_count"]))
    require(int(row["null_event_dates"]) == 0, table, "null event_date count", 0, int(row["null_event_dates"]))
    require(int(row["invalid_fraud_count_rows"]) == 0, table, "fraud_count range", 0, int(row["invalid_fraud_count_rows"]))
    require(int(row["invalid_fraud_rate_rows"]) == 0, table, "fraud_rate range", 0, int(row["invalid_fraud_rate_rows"]))
    require(int(row["inconsistent_fraud_rate_rows"]) == 0, table, "fraud_rate consistency", 0, int(row["inconsistent_fraud_rate_rows"]))
    require(int(row["invalid_high_value_count_rows"]) == 0, table, "high_value_count range", 0, int(row["invalid_high_value_count_rows"]))
    require(int(row["invalid_min_amount_rows"]) == 0, table, "min_amount > 0", 0, int(row["invalid_min_amount_rows"]))
    require(int(row["invalid_amount_order_rows"]) == 0, table, "min/avg/max ordering", 0, int(row["invalid_amount_order_rows"]))
    require(float(row["min_amount"]) > 0, table, "min_amount > 0", "> 0", float(row["min_amount"]))
    require(float(row["min_amount"]) <= float(row["avg_amount"]), table, "min_amount <= avg_amount", "<= avg_amount", (float(row["min_amount"]), float(row["avg_amount"])))
    require(float(row["avg_amount"]) <= float(row["max_amount"]), table, "avg_amount <= max_amount", "<= max_amount", (float(row["avg_amount"]), float(row["max_amount"])))
    print(f"Validated {table}: row count={row['row_count']}, distinct dates={row['distinct_event_dates']}")


def validate_customer_risk_features() -> None:
    table = "customer_risk_features"
    query = f"""
    SELECT
      COUNT(*) AS row_count,
      COUNT(DISTINCT customer_id) AS unique_customer_ids,
      SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) AS null_customer_ids,
      SUM(CASE WHEN transaction_count <= 0 THEN 1 ELSE 0 END) AS invalid_transaction_count_rows,
      SUM(CASE WHEN fraud_count < 0 OR fraud_count > transaction_count THEN 1 ELSE 0 END) AS invalid_fraud_count_rows,
      SUM(CASE WHEN fraud_rate IS NULL OR fraud_rate < 0 OR fraud_rate > 1 THEN 1 ELSE 0 END) AS invalid_fraud_rate_rows,
      SUM(CASE WHEN ABS(fraud_rate - SAFE_DIVIDE(fraud_count, transaction_count)) > 0.000001 THEN 1 ELSE 0 END) AS inconsistent_fraud_rate_rows,
      SUM(CASE WHEN high_value_transaction_count IS NULL OR high_value_transaction_count < 0 OR high_value_transaction_count > transaction_count THEN 1 ELSE 0 END) AS invalid_high_value_count_rows,
      SUM(CASE WHEN active_days IS NULL OR active_days <= 0 OR active_days > transaction_count THEN 1 ELSE 0 END) AS invalid_active_days_rows,
      SUM(CASE WHEN first_seen_at IS NULL OR last_seen_at IS NULL OR first_seen_at > last_seen_at THEN 1 ELSE 0 END) AS invalid_activity_window_rows
    FROM `{PROJECT_ID}.{DATASET_ID}.{table}`
    """
    rows = run_query(query)
    row = rows[0]
    require(int(row["row_count"]) == 500, table, "row count", 500, int(row["row_count"]))
    require(int(row["unique_customer_ids"]) == 500, table, "unique customer IDs", 500, int(row["unique_customer_ids"]))
    require(int(row["null_customer_ids"]) == 0, table, "null customer IDs", 0, int(row["null_customer_ids"]))
    require(int(row["invalid_transaction_count_rows"]) == 0, table, "transaction_count > 0", 0, int(row["invalid_transaction_count_rows"]))
    require(int(row["invalid_fraud_count_rows"]) == 0, table, "fraud_count range", 0, int(row["invalid_fraud_count_rows"]))
    require(int(row["invalid_fraud_rate_rows"]) == 0, table, "fraud_rate range", 0, int(row["invalid_fraud_rate_rows"]))
    require(int(row["inconsistent_fraud_rate_rows"]) == 0, table, "fraud_rate consistency", 0, int(row["inconsistent_fraud_rate_rows"]))
    require(int(row["invalid_high_value_count_rows"]) == 0, table, "high_value_count range", 0, int(row["invalid_high_value_count_rows"]))
    require(int(row["invalid_active_days_rows"]) == 0, table, "active_days range", 0, int(row["invalid_active_days_rows"]))
    require(int(row["invalid_activity_window_rows"]) == 0, table, "activity window", 0, int(row["invalid_activity_window_rows"]))
    print(f"Validated {table}: row count={row['row_count']}, unique customers={row['unique_customer_ids']}")


def validate_merchant_risk_features() -> None:
    table = "merchant_risk_features"
    query = f"""
    SELECT
      COUNT(*) AS row_count,
      COUNT(DISTINCT merchant_id) AS unique_merchant_ids,
      SUM(CASE WHEN merchant_id IS NULL THEN 1 ELSE 0 END) AS null_merchant_ids,
      SUM(CASE WHEN transaction_count <= 0 THEN 1 ELSE 0 END) AS invalid_transaction_count_rows,
      SUM(CASE WHEN fraud_count < 0 OR fraud_count > transaction_count THEN 1 ELSE 0 END) AS invalid_fraud_count_rows,
      SUM(CASE WHEN fraud_rate IS NULL OR fraud_rate < 0 OR fraud_rate > 1 THEN 1 ELSE 0 END) AS invalid_fraud_rate_rows,
      SUM(CASE WHEN ABS(fraud_rate - SAFE_DIVIDE(fraud_count, transaction_count)) > 0.000001 THEN 1 ELSE 0 END) AS inconsistent_fraud_rate_rows,
      SUM(CASE WHEN high_value_transaction_count IS NULL OR high_value_transaction_count < 0 OR high_value_transaction_count > transaction_count THEN 1 ELSE 0 END) AS invalid_high_value_count_rows,
      SUM(CASE WHEN active_days IS NULL OR active_days <= 0 OR active_days > transaction_count THEN 1 ELSE 0 END) AS invalid_active_days_rows,
      SUM(CASE WHEN first_seen_at IS NULL OR last_seen_at IS NULL OR first_seen_at > last_seen_at THEN 1 ELSE 0 END) AS invalid_activity_window_rows
    FROM `{PROJECT_ID}.{DATASET_ID}.{table}`
    """
    rows = run_query(query)
    row = rows[0]
    require(int(row["row_count"]) == 100, table, "row count", 100, int(row["row_count"]))
    require(int(row["unique_merchant_ids"]) == 100, table, "unique merchant IDs", 100, int(row["unique_merchant_ids"]))
    require(int(row["null_merchant_ids"]) == 0, table, "null merchant IDs", 0, int(row["null_merchant_ids"]))
    require(int(row["invalid_transaction_count_rows"]) == 0, table, "transaction_count > 0", 0, int(row["invalid_transaction_count_rows"]))
    require(int(row["invalid_fraud_count_rows"]) == 0, table, "fraud_count range", 0, int(row["invalid_fraud_count_rows"]))
    require(int(row["invalid_fraud_rate_rows"]) == 0, table, "fraud_rate range", 0, int(row["invalid_fraud_rate_rows"]))
    require(int(row["inconsistent_fraud_rate_rows"]) == 0, table, "fraud_rate consistency", 0, int(row["inconsistent_fraud_rate_rows"]))
    require(int(row["invalid_high_value_count_rows"]) == 0, table, "high_value_count range", 0, int(row["invalid_high_value_count_rows"]))
    require(int(row["invalid_active_days_rows"]) == 0, table, "active_days range", 0, int(row["invalid_active_days_rows"]))
    require(int(row["invalid_activity_window_rows"]) == 0, table, "activity window", 0, int(row["invalid_activity_window_rows"]))
    print(f"Validated {table}: row count={row['row_count']}, unique merchants={row['unique_merchant_ids']}")


def validate_segment_risk_summary() -> None:
    table = "segment_risk_summary"
    query = f"""
    WITH distinct_segments AS (
      SELECT DISTINCT country, merchant_category, payment_method
      FROM `{PROJECT_ID}.{DATASET_ID}.{table}`
    )
    SELECT
      (SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_ID}.{table}`) AS row_count,
      (SELECT COUNT(*) FROM distinct_segments) AS unique_segment_combinations,
      SUM(CASE WHEN country IS NULL OR merchant_category IS NULL OR payment_method IS NULL THEN 1 ELSE 0 END) AS null_segment_key_rows,
      SUM(CASE WHEN transaction_count <= 0 THEN 1 ELSE 0 END) AS invalid_transaction_count_rows,
      SUM(CASE WHEN fraud_count < 0 OR fraud_count > transaction_count THEN 1 ELSE 0 END) AS invalid_fraud_count_rows,
      SUM(CASE WHEN fraud_rate IS NULL OR fraud_rate < 0 OR fraud_rate > 1 THEN 1 ELSE 0 END) AS invalid_fraud_rate_rows,
      SUM(CASE WHEN ABS(fraud_rate - SAFE_DIVIDE(fraud_count, transaction_count)) > 0.000001 THEN 1 ELSE 0 END) AS inconsistent_fraud_rate_rows,
      SUM(CASE WHEN high_value_transaction_count IS NULL OR high_value_transaction_count < 0 OR high_value_transaction_count > transaction_count THEN 1 ELSE 0 END) AS invalid_high_value_count_rows,
      SUM(CASE WHEN distinct_customers IS NULL OR distinct_customers <= 0 OR distinct_customers > transaction_count THEN 1 ELSE 0 END) AS invalid_distinct_customer_rows,
      SUM(CASE WHEN distinct_merchants IS NULL OR distinct_merchants <= 0 OR distinct_merchants > transaction_count THEN 1 ELSE 0 END) AS invalid_distinct_merchant_rows,
      SUM(transaction_count) AS total_transaction_count
    FROM `{PROJECT_ID}.{DATASET_ID}.{table}`
    """
    rows = run_query(query)
    row = rows[0]
    require(int(row["row_count"]) == 140, table, "row count", 140, int(row["row_count"]))
    require(int(row["unique_segment_combinations"]) == 140, table, "unique segment combinations", 140, int(row["unique_segment_combinations"]))
    require(int(row["null_segment_key_rows"]) == 0, table, "null composite key fields", 0, int(row["null_segment_key_rows"]))
    require(int(row["invalid_transaction_count_rows"]) == 0, table, "transaction_count > 0", 0, int(row["invalid_transaction_count_rows"]))
    require(int(row["invalid_fraud_count_rows"]) == 0, table, "fraud_count range", 0, int(row["invalid_fraud_count_rows"]))
    require(int(row["invalid_fraud_rate_rows"]) == 0, table, "fraud_rate range", 0, int(row["invalid_fraud_rate_rows"]))
    require(int(row["inconsistent_fraud_rate_rows"]) == 0, table, "fraud_rate consistency", 0, int(row["inconsistent_fraud_rate_rows"]))
    require(int(row["invalid_high_value_count_rows"]) == 0, table, "high_value_count range", 0, int(row["invalid_high_value_count_rows"]))
    require(int(row["invalid_distinct_customer_rows"]) == 0, table, "distinct_customers range", 0, int(row["invalid_distinct_customer_rows"]))
    require(int(row["invalid_distinct_merchant_rows"]) == 0, table, "distinct_merchants range", 0, int(row["invalid_distinct_merchant_rows"]))
    require(int(row["total_transaction_count"]) == 100350, table, "SUM(transaction_count)", 100350, int(row["total_transaction_count"]))
    print(f"Validated {table}: row count={row['row_count']}, unique segments={row['unique_segment_combinations']}")


def validate_high_risk_transactions() -> None:
    table = "high_risk_transactions"
    query = f"""
    SELECT
      COUNT(*) AS row_count,
      COUNT(DISTINCT transaction_id) AS unique_transaction_ids,
      SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) AS null_transaction_ids,
      SUM(CASE WHEN is_fraud IS NULL OR amount IS NULL OR ((NOT is_fraud) AND amount < 1000) THEN 1 ELSE 0 END) AS invalid_high_risk_rows,
      SUM(CASE WHEN risk_reason IS NULL OR risk_reason NOT IN ('known_fraud_and_high_value', 'known_fraud', 'high_value') THEN 1 ELSE 0 END) AS invalid_risk_reason_rows,
      SUM(CASE WHEN risk_reason = 'known_fraud_and_high_value' AND (NOT is_fraud OR amount < 1000) THEN 1 ELSE 0 END) AS invalid_fraud_and_high_value_rows,
      SUM(CASE WHEN risk_reason = 'known_fraud' AND ((NOT is_fraud) OR amount >= 1000) THEN 1 ELSE 0 END) AS invalid_known_fraud_rows,
      SUM(CASE WHEN risk_reason = 'high_value' AND (is_fraud OR amount < 1000) THEN 1 ELSE 0 END) AS invalid_high_value_rows,
      SUM(CASE WHEN event_date IS NULL THEN 1 ELSE 0 END) AS null_event_dates,
      COUNT(DISTINCT event_date) AS distinct_event_dates
    FROM `{PROJECT_ID}.{DATASET_ID}.{table}`
    """
    rows = run_query(query)
    row = rows[0]
    require(int(row["row_count"]) == 3289, table, "row count", 3289, int(row["row_count"]))
    require(int(row["unique_transaction_ids"]) == 3289, table, "unique transaction IDs", 3289, int(row["unique_transaction_ids"]))
    require(int(row["null_transaction_ids"]) == 0, table, "null transaction IDs", 0, int(row["null_transaction_ids"]))
    require(int(row["invalid_high_risk_rows"]) == 0, table, "high-risk row logic", 0, int(row["invalid_high_risk_rows"]))
    require(int(row["invalid_risk_reason_rows"]) == 0, table, "risk_reason values", 0, int(row["invalid_risk_reason_rows"]))
    require(int(row["invalid_fraud_and_high_value_rows"]) == 0, table, "known_fraud_and_high_value logic", 0, int(row["invalid_fraud_and_high_value_rows"]))
    require(int(row["invalid_known_fraud_rows"]) == 0, table, "known_fraud logic", 0, int(row["invalid_known_fraud_rows"]))
    require(int(row["invalid_high_value_rows"]) == 0, table, "high_value logic", 0, int(row["invalid_high_value_rows"]))
    require(int(row["null_event_dates"]) == 0, table, "null event_date", 0, int(row["null_event_dates"]))

    partition_query = f"""
    SELECT COUNT(*) AS partition_count
    FROM `{PROJECT_ID}.{DATASET_ID}.INFORMATION_SCHEMA.PARTITIONS`
    WHERE table_name = '{table}'
    """
    partition_rows = run_query(partition_query)
    partition_count = int(partition_rows[0]["partition_count"])
    require(partition_count > 0, table, "partition metadata exists", "> 0", partition_count)
    require(partition_count == int(row["distinct_event_dates"]), table, "partition count", int(row["distinct_event_dates"]), partition_count)
    print(f"Validated {table}: row count={row['row_count']}, partition count={partition_count}")


def main() -> None:
    print("Running BigQuery warehouse validation...")
    validate_processed_transactions()
    validate_daily_transaction_summary()
    validate_customer_risk_features()
    validate_merchant_risk_features()
    validate_segment_risk_summary()
    validate_high_risk_transactions()
    print("BIGQUERY WAREHOUSE VALIDATION COMPLETE")


if __name__ == "__main__":
    main()
