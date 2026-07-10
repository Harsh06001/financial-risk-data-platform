import subprocess


PROJECT_ID = "risk-data-platform-npg-2026"
LOCATION = "us-central1"
DATASET_ID = "risk_analytics"

ANALYTICS_BUCKET = "risk-data-platform-npg-2026-analytics-data"
RISK_FEATURES_URI = f"gs://{ANALYTICS_BUCKET}/risk_features"

def run_command(command: list[str]) -> None:
    print(f"\nRunning: {' '.join(command)}")
    subprocess.run(command, check=True)


STANDARD_TABLES = {
    "daily_transaction_summary": (
        f"{RISK_FEATURES_URI}/daily_transaction_summary/*.parquet"
    ),
    "customer_risk_features": (
        f"{RISK_FEATURES_URI}/customer_risk_features/*.parquet"
    ),
    "merchant_risk_features": (
        f"{RISK_FEATURES_URI}/merchant_risk_features/*.parquet"
    ),
    "segment_risk_summary": (
        f"{RISK_FEATURES_URI}/segment_risk_summary/*.parquet"
    ),
}


HIGH_RISK_TABLE = "high_risk_transactions"

HIGH_RISK_SOURCE_URI = (
    f"{RISK_FEATURES_URI}/{HIGH_RISK_TABLE}/*.parquet"
)

HIGH_RISK_SOURCE_PREFIX = (
    f"{RISK_FEATURES_URI}/{HIGH_RISK_TABLE}/"
)




if __name__ == "__main__":
    run_command(["bq", "version"])

def load_standard_table(table_name: str, source_uri: str) -> None:
    destination_table = f"{DATASET_ID}.{table_name}"

    command = [
        "bq",
        f"--project_id={PROJECT_ID}",
        f"--location={LOCATION}",
        "load",
        "--replace",
        "--source_format=PARQUET",
        destination_table,
        source_uri,
    ]

    run_command(command)


def load_standard_tables() -> None:
    for table_name, source_uri in STANDARD_TABLES.items():
        load_standard_table(table_name, source_uri)


def load_high_risk_table() -> None:
    destination_table = f"{DATASET_ID}.{HIGH_RISK_TABLE}"

    command = [
        "bq",
        f"--project_id={PROJECT_ID}",
        f"--location={LOCATION}",
        "load",
        "--replace",
        "--source_format=PARQUET",
        "--hive_partitioning_mode=AUTO",
        f"--hive_partitioning_source_uri_prefix={HIGH_RISK_SOURCE_PREFIX}",
        "--time_partitioning_type=DAY",
        "--time_partitioning_field=event_date",
        destination_table,
        HIGH_RISK_SOURCE_URI,
    ]

    run_command(command)


if __name__ == "__main__":
    load_standard_tables()
    load_high_risk_table()