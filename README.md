# Financial Risk Data Platform

A batch-oriented GCP data engineering portfolio project with schema-enforced PySpark processing, partitioned Parquet, guarded GCS synchronization, native BigQuery warehousing, dbt dimensional modeling, Airflow orchestration, and Terraform-managed infrastructure.

## Architecture

```mermaid
flowchart LR
    A[Raw CSV] --> B{Processing mode}
    B -->|Full refresh| C[PySpark ETL + risk features]
    B -->|--event-date| D[One-date PySpark ETL]
    C --> E[Processed + analytics Parquet]
    D --> F[Selected processed partition]
    E --> G[Guarded GCS mirrors]
    F --> H[Exact GCS partition sync]
    G --> I[BigQuery native tables]
    H --> J[Staging table + MERGE]
    J --> I
    I --> K[dbt staging]
    K --> L[fct_transactions + dimensions]
    L --> M[Risk marts]
    E -.-> N[BigQuery external-table demonstration]
```

The dimensional core contains `fct_transactions` at one row per `transaction_id`, `dim_customer` at one row per `customer_id`, `dim_merchant` at one row per `merchant_id`, and `dim_date` at one row per observed `event_date`. Stable source natural keys are used; v1.1 intentionally models current state rather than SCD Type 2 history.

## Run the pipelines

Prerequisites are authenticated Google Application Default Credentials, provisioned infrastructure, `.venv` for the Python/Spark requirements, and `.venv-dbt2` for `requirements-dbt.txt`. Terraform execution is separate from pipeline execution.

Full cloud pipeline—local batch validation, narrow GCS mirrors, native BigQuery loads and validation, then dbt run/test:

```bash
.venv/bin/python pipeline/run_cloud_pipeline.py
```

One-date incremental pipeline—dynamic partition overwrite, exact partition sync, BigQuery staging-and-`MERGE`, then dbt run/test:

```bash
.venv/bin/python pipeline/run_incremental_pipeline.py --event-date 2026-07-08
```

The same event date can be rerun safely. The warehouse `MERGE` matches on `transaction_id`, updates matched rows, and inserts new rows.

Useful verification commands:

```bash
.venv/bin/python warehouse/validate_bigquery_tables.py
.venv-dbt2/bin/dbt debug --project-dir dbt/risk_analytics --profiles-dir dbt/risk_analytics
.venv-dbt2/bin/dbt run --project-dir dbt/risk_analytics --profiles-dir dbt/risk_analytics
.venv-dbt2/bin/dbt test --project-dir dbt/risk_analytics --profiles-dir dbt/risk_analytics
.venv/bin/python benchmarks/run_scale_tests.py --sizes 1000000 5000000 10000000
```

`DBT_EXECUTABLE` can point the pipeline runners and Airflow DAG to another compatible dbt executable. The committed dbt profile uses OAuth and environment-variable configuration; it contains no key file or credential.

## Verified evidence

Canonical validation reconciles 100,350 processed rows and unique transaction IDs across 31 event dates (2026-06-08 through 2026-07-08), including 3,261 fraud rows. Five feature tables and the native `processed_transactions` table feed the dbt layers.

The tracked 100,350-row Spark strategy comparison reduced Parquet output from 248 to 31 files (87.5%), reduced output size from 7.86 MB to 5.99 MB (23.8%), and changed median local runtime from 7.95 to 7.51 seconds (5.5%). See [`benchmarks/results/batch_100k_strategy_comparison.csv`](benchmarks/results/batch_100k_strategy_comparison.csv).

Separate deterministic **local synthetic scale tests** completed and reconciled:

| Rows | Dates | Runtime | Rows/s | Parquet files | Output bytes |
|---:|---:|---:|---:|---:|---:|
| 1,000,000 | 31 | 14.7662s | 67,722.09 | 31 | 52,784,849 |
| 5,000,000 | 31 | 29.4523s | 169,766.16 | 31 | 243,147,767 |
| 10,000,000 | 31 | 45.4968s | 219,795.88 | 31 | 470,591,204 |

These are local `Spark local[*]` results on the recorded 8-CPU environment, not production or distributed-cluster claims. Evidence is in [`scale_test_results.csv`](benchmarks/results/scale_test_results.csv) and [`scale_test_report.md`](benchmarks/results/scale_test_report.md). The isolated late-data test evidence is in [`incremental_merge_evidence.csv`](benchmarks/results/incremental_merge_evidence.csv).

## Documentation

Start with the [documentation index](docs/README.md) or the guided [learning path](docs/LEARNING_PATH.md). The deep documentation covers architecture, contracts, every major file and function, Spark internals, GCS safety, BigQuery, dimensional modeling, incremental MERGE behavior, dbt, Airflow, scale tests, failure scenarios, production trade-offs, interview preparation, and claim-safe [resume evidence](docs/resume/verified-project-bullets.md).

## Known limitations

- This is a local-development portfolio system, not a production SLA or distributed-cluster benchmark.
- Full GCS synchronization is mirror-style rather than an atomic generation swap; its delete-enabled operation is restricted by exact bucket/prefix guards and inventory checks.
- Inventory validation compares object paths and counts, not content checksums.
- The incremental transaction fact uses a processed timestamp watermark; a full refresh remains available for historical rebuilds.
- Dimensions are current-state natural-key models, not SCD Type 2 history.
- Airflow DAG syntax and task wiring are repository-validated, but an Airflow scheduler runtime is not bundled with the project.
- The isolated late-arrival demonstration creates dedicated cloud test resources; this work does not delete cloud datasets or buckets.
