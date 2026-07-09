import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]

BATCH_PROCESSING_DIR = PROJECT_ROOT / "batch-processing"

sys.path.insert(0, str(BATCH_PROCESSING_DIR))


from process_transactions import (  # noqa: E402
    clean_transactions,
    create_spark_session,
    read_raw_transactions,
    transform_transactions,
)


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw_df = read_raw_transactions(
            spark=spark,
            input_path=str(
                PROJECT_ROOT
                / "data"
                / "raw"
                / "transactions"
                / "*.csv"
            ),
        )

        processed_df = transform_transactions(raw_df)

        cleaned_df = clean_transactions(processed_df)

        baseline_df = cleaned_df

        repartitioned_df = cleaned_df.repartition(
            "event_date"
        )

        print()
        print("=" * 80)
        print("BASELINE PHYSICAL PLAN")
        print("=" * 80)

        baseline_df.explain(mode="formatted")

        print()
        print("=" * 80)
        print("REPARTITIONED PHYSICAL PLAN")
        print("=" * 80)

        repartitioned_df.explain(mode="formatted")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()