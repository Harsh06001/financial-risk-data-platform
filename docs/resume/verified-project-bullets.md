# Verified Project Resume Bullets

These bullets describe the Financial Risk Data Platform project, not professional production experience. Select two or three bullets that match the target role rather than using every option.

## Version 1.2 Evidence — VERIFIED IN REPOSITORY

- Added a Dockerized local development configuration with a single Kafka-compatible Redpanda broker, reproducible Make commands, mounted generated-data volumes, and documented local/cloud-enabled modes.
- Built a controlled Spark Structured Streaming path with an explicit transaction schema, bounded execution, checkpointing, invalid-record quarantine, per-micro-batch duplicate handling, reconciliation metrics, and an optional staged BigQuery `MERGE`.
- Implemented local-first data observations and console/JSON/optional Slack alert channels, plus a separate five-task Airflow monitoring DAG.
- Added credential-free GitHub Actions validation, a no-push Docker build workflow, and a manual credential-gated GCP integration workflow.

Only use runtime wording (for example, “verified Docker build” or “loaded streaming data into BigQuery”) when a retained execution result supports it.

### DO NOT CLAIM

Production streaming, production monitoring/alerting, enterprise CI/CD, managed Kafka/Spark/Airflow, Kubernetes, real-time fraud detection, measured streaming throughput, or professional production use.

## Version 1.3 Evidence — VERIFIED IN REPOSITORY

- Extended the controlled streaming design with idempotent input/DLQ/risk topics, optional Spark Kafka sinks, batch accounting, and checkpoint restart verification.
- Added a local Prometheus, Grafana, and Alertmanager deployment definition with Redpanda/application metrics, three provisioned dashboards, nine data-platform alert rules, and console/file/optional-Slack routing.
- Added an opt-in streaming event-quality mart with reconciliation tests and a manual, credential-free container integration workflow.

Say “implemented a local deployment definition” until a retained successful runtime result supports stronger “ran” or “verified” wording.

## Evidence rules

- `VERIFIED` means the repository contains an implementation and a successful command result or tracked evidence artifact.
- Keep the phrase **local synthetic** with the 1M/5M/10M scale results.
- Do not combine the scale-test throughput with the separate 100,350-row write-strategy comparison.
- Re-run cloud validations before using a claim if infrastructure or source data changes.

## General Data Engineer

VERIFIED:

- Built a rerunnable batch data platform using PySpark, Parquet, GCS, BigQuery, dbt, Airflow, and Terraform, with fail-fast validation across raw, processed, warehouse, and transformation layers.
- Implemented full-refresh and event-date incremental paths that preserve partition isolation, load through a BigQuery staging table, and `MERGE` on `transaction_id` to prevent duplicate inserts and apply corrected records.
- Designed a dimensional layer with an incremental transaction fact and customer, merchant, and date dimensions; enforced grain, null, uniqueness, relationship, and source-to-fact reconciliation tests.
- Reconciled local synthetic PySpark runs at 1M, 5M, and 10M rows with 31 date partitions and no input/output row loss; the 10M run completed in 45.4968 seconds on an 8-CPU local environment.

Evidence: [`scale_test_results.csv`](../../benchmarks/results/scale_test_results.csv), [`scale_test_report.md`](../../benchmarks/results/scale_test_report.md), and [`incremental_merge_evidence.csv`](../../benchmarks/results/incremental_merge_evidence.csv).

## AWS / Spark Data Engineer

This project is implemented on GCP, so AWS belongs in professional-experience bullets—not in this project’s technology list. Spark-focused options are:

VERIFIED:

- Developed schema-enforced PySpark ETL for transaction cleaning, deduplication, derived risk fields, feature aggregation, and event-date-partitioned Parquet output with row- and grain-level reconciliation.
- Reduced Parquet output from 248 files to 31 by aligning final writer tasks with 31 event-date partitions, an 87.5% file-count reduction; median local runtime improved from 7.95 to 7.51 seconds in the tracked 100,350-row comparison.
- Built a deterministic local scale harness and successfully reconciled 1M, 5M, and 10M-row batches using Spark `local[*]`, recording runtime, throughput, partition count, file count, output bytes, and environment metadata.

Evidence: [`batch_100k_strategy_comparison.csv`](../../benchmarks/results/batch_100k_strategy_comparison.csv), [`batch_write_file_count_investigation.md`](../../benchmarks/results/batch_write_file_count_investigation.md), and [`scale_test_report.md`](../../benchmarks/results/scale_test_report.md).

## GCP / BigQuery Data Engineer

VERIFIED:

- Implemented guarded, idempotent GCS synchronization for processed and analytics Parquet prefixes, including exact bucket/prefix checks, source preflight validation, and post-sync inventory reconciliation.
- Loaded six native BigQuery warehouse tables with explicit schemas and partition metadata validation, including a transaction table partitioned by `event_date`, while retaining an external-table pattern for direct GCS querying.
- Added partition-scoped incremental loading through a BigQuery staging table and deterministic `MERGE`, proving unchanged same-date reruns, one late insert, one corrected update, and zero duplicate transaction IDs in an isolated integration scenario.
- Provisioned GCP storage and warehouse resources as code with Terraform and separated infrastructure changes from pipeline execution.

Evidence: [`incremental_merge_evidence.csv`](../../benchmarks/results/incremental_merge_evidence.csv), [`10-bigquery-warehouse.md`](../10-bigquery-warehouse.md), and [`13-bigquery-merge-and-late-data.md`](../13-bigquery-merge-and-late-data.md).

## Analytics Engineer / dbt

VERIFIED:

- Built a dbt project that transforms six BigQuery staging views into a tested star-schema-style core layer and five risk-analysis marts.
- Materialized `fct_transactions` incrementally with BigQuery `merge`, `transaction_id` as the unique key, and `event_date` partitioning, while keeping small current-state dimensions and marts simple.
- Added tests for model grain, required fields, dimension relationships, source contracts, high-risk logic, segment grain, daily reconciliation, and fact-to-processed transaction reconciliation.

Evidence: [`dbt/risk_analytics/models`](../../dbt/risk_analytics/models), [`dbt/risk_analytics/tests`](../../dbt/risk_analytics/tests), and the final verified dbt command results recorded in the project handoff.

## Do not claim

DO NOT CLAIM:

- production traffic, production SLAs, production cost savings, revenue impact, or customer impact;
- distributed-cluster performance from the local `local[*]` scale tests;
- that 10M rows is the platform’s maximum capacity;
- AWS, Kafka, streaming, Kubernetes, Dataflow, Databricks, Snowflake, Redshift, or machine learning in this project;
- SCD Type 2, CDC, exactly-once delivery, atomic multi-system commits, or checksum-based GCS content validation;
- a dbt model/test count unless it matches a fresh successful `dbt run` and `dbt test`.
