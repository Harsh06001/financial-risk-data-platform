# Real-Time Financial Risk Data Platform

An end-to-end data engineering platform for batch and real-time financial transaction processing.

## Planned Architecture

- Google Cloud Storage for raw and processed data
- PySpark for distributed batch processing
- BigQuery for analytical warehousing
- dbt for transformation and dimensional modeling
- Apache Airflow for orchestration
- Apache Kafka for real-time event ingestion
- Spark Structured Streaming for stream processing
- Terraform for infrastructure provisioning
- Docker for reproducible local services
- GitHub Actions for CI/CD

## Project Status

Currently building the local batch-processing and risk analytics foundation.

## Local Batch Workflow

Run the validated local batch pipeline:

```bash
.venv/bin/python batch-processing/run_batch_pipeline.py --expected-event-dates 31
```

Build analytics-ready risk feature tables:

```bash
.venv/bin/python batch-processing/build_risk_features.py
```

Validate an existing processed Parquet output:

```bash
.venv/bin/python batch-processing/validate_processed_transactions.py \
  --input data/processed/transactions \
  --expected-rows 100350 \
  --expected-event-dates 31
```
