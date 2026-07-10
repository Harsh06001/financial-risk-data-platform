# Data Engineer Keyword Map

This map links truthful resume keywords to concrete implementation evidence. It is not a recommendation to paste every keyword into one resume.

| Keyword | Implemented evidence | Best resume context |
|---|---|---|
| Apache Spark / PySpark | Schema-enforced processing, validation, feature engineering, partition writes | General DE, Spark DE |
| Spark SQL | DataFrame expressions, aggregations, windows, joins, validation queries | Spark DE |
| Batch Processing | Separate full and event-date batch entry points | All DE variants |
| ETL | Raw CSV to cleaned, derived, partitioned Parquet | General DE |
| ELT | Native BigQuery sources transformed by dbt | Analytics DE |
| Data Lake | Raw, processed, and analytics GCS layers | Cloud DE |
| Parquet | Processed and feature outputs with explicit schemas | Spark / cloud DE |
| Partitioning | Hive-style `event_date=...` Parquet and BigQuery date partitions | Spark / BigQuery DE |
| Partition Pruning | Date-scoped Spark input and BigQuery partition-aware design | Spark / BigQuery DE |
| Small-Files Optimization | Tracked 248-to-31 Parquet file comparison | Spark DE |
| Data Quality / Data Contracts | Null, range, grain, duplicate, partition, and reconciliation checks | All variants |
| Reconciliation | Raw-to-processed, processed-to-BigQuery, source-to-fact checks | All variants |
| Idempotency | Guarded mirror sync, replace loads, same-date MERGE reruns | Cloud DE |
| Incremental Processing | `--event-date` pipeline with isolated partition processing | BigQuery / analytics DE |
| MERGE / Upsert | BigQuery staging-and-MERGE on `transaction_id` | BigQuery DE |
| Late-Arriving Data | Isolated late insert and corrected-record integration scenario | BigQuery DE |
| Google Cloud Storage (GCS) | Guarded processed/analytics and partition-scoped synchronization | GCP DE |
| BigQuery | Native tables, partitioning, validation, staging tables, MERGE | GCP DE |
| External Tables | Terraform-managed BigQuery table over analytics Parquet | GCP DE |
| Data Warehousing | Native warehouse layer feeding dbt | General / GCP DE |
| dbt | Staging, core, marts, incremental materialization, tests | Analytics DE |
| Data Modeling | Explicit source, staging, core, and mart grains | Analytics DE |
| Dimensional Modeling / Star Schema | Transaction fact plus customer, merchant, and date dimensions | Analytics DE |
| Fact Tables / Dimension Tables | `fct_transactions`, `dim_customer`, `dim_merchant`, `dim_date` | Analytics DE |
| Natural Keys | Stable source identifiers used without fabricated surrogate keys | Analytics DE |
| Airflow | Seven-task scheduled batch DAG calling project scripts | Orchestration-focused roles |
| Orchestration / Retries / Failure Propagation | Linear task graph, retries, nonzero exit propagation | General DE |
| Terraform / Infrastructure as Code | GCP buckets, dataset, and table configuration | Cloud DE |
| Synthetic Scale Testing | Deterministic 1M/5M/10M local reconciliation runs | Spark DE |
| Python | Pipeline runners, generators, GCS/BQ utilities, validators | All variants |
| SQL | BigQuery MERGE, dbt models, singular tests | BigQuery / analytics DE |
| CI-ready Validation | Deterministic syntax, dbt, Terraform, diff, and evidence checks | General DE; do not call it CI/CD unless wired to CI |

## Intentionally excluded

The repository does not implement Kafka, streaming, Kubernetes, Dataflow, Databricks, Snowflake, Redshift, machine learning, SCD Type 2, or a production CDC system. Do not add those terms for keyword matching.
