# Project Overview

The platform processes synthetic financial transactions as a batch data-engineering system. It demonstrates explicit schemas, PySpark ETL, partitioned Parquet, feature engineering, safe GCS synchronization, native and external BigQuery tables, dbt dimensional modeling, incremental MERGE behavior, testing, orchestration, Terraform, and honest local scale evidence.

The two supported operating modes are:

- Full refresh: validate all raw CSV files, rebuild processed transactions and five feature tables, mirror them to GCS, replace native BigQuery tables, then run dbt.
- Daily incremental: select one `event_date`, dynamically overwrite only that local partition, mirror only that GCS prefix, stage it in BigQuery, MERGE on `transaction_id`, then run dbt.

The canonical verified dataset has 100,350 unique transactions, 3,261 fraud rows, and 31 dates from 2026-06-08 through 2026-07-08. Five feature tables support risk analysis. The core dimensional layer adds `fct_transactions`, `dim_customer`, `dim_merchant`, and `dim_date`.

This is a local-development portfolio system, not a claim of production traffic or distributed-cluster scale. The 1M, 5M, and 10M results are explicitly local synthetic scale tests.
