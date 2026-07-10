# PySpark Batch ETL

`process_transactions.py` applies an explicit `StructType`, parses timestamps with `try_to_timestamp`, derives date/hour metadata, filters invalid IDs/timestamps/amounts, and deduplicates by `transaction_id`. Transformations are lazy; calls such as `count`, `show`, and Parquet writes trigger Spark jobs.

Full mode writes all partitions with overwrite. Incremental mode filters one validated date and explicitly sets dynamic partition overwrite so unrelated directories survive. An empty selected date fails before the write.

```bash
.venv/bin/python batch-processing/process_transactions.py \
  --input 'data/raw/transactions/*.csv' \
  --output data/processed/transactions \
  --write-strategy repartitioned

.venv/bin/python batch-processing/process_transactions.py \
  --event-date 2026-07-08
```

Spark SQL uses UTC by default so offset-aware source timestamps produce deterministic `event_date` and `event_hour` values on every host. `SPARK_SQL_SESSION_TIMEZONE` remains an explicit override for a documented business-timezone requirement; the scale harness also pins it to UTC in its recorded environment.

At 1 GB, local execution is reasonable. At 1 TB, use a managed cluster, partition pruning, distributed input files, and tuned executor/shuffle sizing. At 100 TB, add catalog-managed tables, adaptive resource policies, compaction, observability, and incremental ingestion boundaries.
