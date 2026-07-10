import argparse
import csv
import subprocess
import sys
from datetime import date
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE_DIR = PROJECT_ROOT / "warehouse"
sys.path.insert(0, str(WAREHOUSE_DIR))

from load_bigquery_tables import DATASET_ID, LOCATION, PROJECT_ID, run_command  # noqa: E402
from merge_processed_partition import merge_processed_partition, table_metrics  # noqa: E402
from validate_bigquery_tables import run_query  # noqa: E402


DEMO_ROOT = PROJECT_ROOT / "data" / "incremental-demo"
DEMO_PROCESSED_ROOT = DEMO_ROOT / "processed" / "transactions"
FIXTURE_PATH = DEMO_ROOT / "fixtures" / "late_arriving_transactions.csv"
EVIDENCE_PATH = (
    PROJECT_ROOT / "benchmarks" / "results" / "incremental_merge_evidence.csv"
)
TARGET_TABLE = "processed_transactions_incremental_demo"


def parse_event_date(value: str) -> str:
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "event date must use YYYY-MM-DD format"
        ) from exc


def run_project_command(command: list[str]) -> None:
    subprocess.run(command, cwd=PROJECT_ROOT, check=True)


def initialize_demo_target(event_date: str) -> int:
    query = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_ID}.{TARGET_TABLE}`
    PARTITION BY event_date AS
    SELECT *
    FROM `{PROJECT_ID}.{DATASET_ID}.processed_transactions`
    WHERE event_date = DATE('{event_date}')
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
    return table_metrics(TARGET_TABLE)["row_count"]


def process_demo_batch(event_date: str, raw_input: str) -> None:
    run_project_command(
        [
            sys.executable,
            "batch-processing/process_transactions.py",
            "--input",
            raw_input,
            "--output",
            str(DEMO_PROCESSED_ROOT),
            "--write-strategy",
            "repartitioned",
            "--event-date",
            event_date,
        ]
    )
    run_project_command(
        [
            sys.executable,
            "batch-processing/validate_processed_transactions.py",
            "--input",
            str(DEMO_PROCESSED_ROOT),
            "--expected-event-dates",
            "1",
            "--event-date",
            event_date,
        ]
    )
    run_project_command(
        [
            sys.executable,
            "cloud/sync_processed_partition_to_gcs.py",
            "--event-date",
            event_date,
            "--scope",
            "demo",
        ]
    )


def find_source_row(event_date: str) -> dict[str, str]:
    canonical_row = run_query(
        f"""
        SELECT transaction_id
        FROM `{PROJECT_ID}.{DATASET_ID}.processed_transactions`
        WHERE event_date = DATE('{event_date}')
        ORDER BY transaction_id
        LIMIT 1
        """
    )[0]
    canonical_transaction_id = canonical_row["transaction_id"]

    for input_path in sorted(
        (PROJECT_ROOT / "data" / "raw" / "transactions").glob("*.csv")
    ):
        with input_path.open(newline="", encoding="utf-8") as csv_file:
            for row in csv.DictReader(csv_file):
                if row["transaction_id"] == canonical_transaction_id:
                    return row
    raise RuntimeError(
        f"No canonical raw row found for transaction {canonical_transaction_id}"
    )


def create_late_fixture(event_date: str) -> tuple[str, str, float]:
    source_row = find_source_row(event_date)
    corrected = dict(source_row)
    corrected_amount = round(float(source_row["amount"]) + 1.0, 2)
    corrected.update(
        {
            "event_timestamp": f"{event_date}T12:00:00+00:00",
            "amount": f"{corrected_amount:.2f}",
        }
    )

    late_transaction_id = f"LATE_{event_date.replace('-', '')}_000001"
    late = dict(source_row)
    late.update(
        {
            "transaction_id": late_transaction_id,
            "event_timestamp": f"{event_date}T23:59:59+00:00",
            "amount": "123.45",
            "is_fraud": "False",
        }
    )

    FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with FIXTURE_PATH.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=list(source_row))
        writer.writeheader()
        writer.writerows([corrected, late])

    return source_row["transaction_id"], late_transaction_id, corrected_amount


def verify_late_merge(
    corrected_transaction_id: str,
    late_transaction_id: str,
    corrected_amount: float,
) -> tuple[int, int, int]:
    query = f"""
    SELECT
      COUNT(*) - COUNT(DISTINCT transaction_id) AS duplicate_rows,
      COUNTIF(transaction_id = '{late_transaction_id}') AS late_rows,
      COUNTIF(
        transaction_id = '{corrected_transaction_id}'
        AND ABS(amount - {corrected_amount}) < 0.000001
      ) AS corrected_rows
    FROM `{PROJECT_ID}.{DATASET_ID}.{TARGET_TABLE}`
    """
    row = run_query(query)[0]
    return (
        int(row["duplicate_rows"]),
        int(row["late_rows"]),
        int(row["corrected_rows"]),
    )


def write_evidence(evidence: dict[str, object]) -> None:
    EVIDENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with EVIDENCE_PATH.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=list(evidence))
        writer.writeheader()
        writer.writerow(evidence)


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify same-date idempotency and late-arriving MERGE behavior."
    )
    parser.add_argument(
        "--event-date",
        default="2026-07-08",
        type=parse_event_date,
    )
    return parser.parse_args()


def main() -> None:
    args = parse_arguments()
    canonical_input = "data/raw/transactions/*.csv"
    baseline_rows = initialize_demo_target(args.event_date)

    process_demo_batch(args.event_date, canonical_input)
    first_merge = merge_processed_partition(args.event_date, "demo")
    process_demo_batch(args.event_date, canonical_input)
    second_merge = merge_processed_partition(args.event_date, "demo")

    if first_merge["after_rows"] != baseline_rows:
        raise RuntimeError("Initial same-date MERGE changed the demo row count")
    if second_merge["after_rows"] != baseline_rows:
        raise RuntimeError("Unchanged same-date rerun increased the demo row count")

    corrected_id, late_id, corrected_amount = create_late_fixture(args.event_date)
    process_demo_batch(args.event_date, str(FIXTURE_PATH))
    late_merge = merge_processed_partition(args.event_date, "demo")
    duplicate_rows, late_rows, corrected_rows = verify_late_merge(
        corrected_id,
        late_id,
        corrected_amount,
    )

    if late_merge["after_rows"] != baseline_rows + 1:
        raise RuntimeError("Late-arriving MERGE did not add exactly one row")
    if (duplicate_rows, late_rows, corrected_rows) != (0, 1, 1):
        raise RuntimeError(
            "Late-arriving verification failed: expected no duplicates, one "
            "late insert, and one corrected update"
        )

    evidence = {
        "event_date": args.event_date,
        "baseline_rows": baseline_rows,
        "first_merge_rows": first_merge["after_rows"],
        "same_date_rerun_rows": second_merge["after_rows"],
        "late_merge_rows": late_merge["after_rows"],
        "rows_added_by_late_batch": late_merge["after_rows"] - baseline_rows,
        "duplicate_rows": duplicate_rows,
        "late_transaction_rows": late_rows,
        "corrected_transaction_rows": corrected_rows,
        "verification": "PASSED",
    }
    write_evidence(evidence)
    print(f"INCREMENTAL MERGE VERIFICATION PASSED: {evidence}")
    print(f"Evidence written to: {EVIDENCE_PATH}")


if __name__ == "__main__":
    main()
