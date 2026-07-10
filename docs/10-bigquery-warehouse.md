# BigQuery Warehouse

`warehouse/load_bigquery_tables.py` loads six native tables: complete processed transactions plus five feature tables. Transaction-level and high-risk tables use Hive partition recovery and BigQuery daily partitioning. Loads use `--replace`, so the full path is rerunnable.

`processed_transactions` preserves the explicit processed schema and one row per transaction. `warehouse/validate_bigquery_tables.py` checks 100,350 rows/IDs, 3,261 fraud rows, 31 dates/partitions, date bounds, required keys, positive amounts, and valid hours, then validates every feature table.

Native tables store BigQuery-managed data and serve dbt predictably. `daily_transaction_summary_external` reads Parquet directly from GCS and demonstrates external-table trade-offs: less loading, but runtime dependence on external files and different performance/management behavior.

Full loading is sequential, not atomic across all tables. At 1 TB, use load jobs with partition controls and cost monitoring. At 100 TB, publish versioned generations or use transactional metadata patterns so consumers do not see mixed refresh generations.
