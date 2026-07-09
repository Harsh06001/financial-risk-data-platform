# Batch Write File Count Investigation

## Context

The batch-processing benchmark compares two write strategies in
`batch-processing/process_transactions.py`.

- `baseline`: write cleaned transactions directly with
  `.partitionBy("event_date").parquet(...)`
- `repartitioned`: call `.repartition("event_date")` before the same
  partitioned Parquet write

The transformation semantics are otherwise identical. Both strategies enforce
the same schema, derive `event_date`, filter invalid rows, and deduplicate by
`transaction_id`.

## Benchmark Result

The verified benchmark output in
`benchmarks/results/batch_100k_strategy_comparison.csv` shows:

- Input rows: 100,350
- Output rows: 100,350 for both strategies
- Distinct event dates: 31
- Baseline median runtime: 7.95 seconds
- Repartitioned median runtime: 7.51 seconds
- Baseline Parquet files: 248
- Repartitioned Parquet files: 31
- Baseline output size: 7.86 MB
- Repartitioned output size: 5.99 MB

## Physical Plan Evidence

The baseline physical plan includes:

```text
Exchange hashpartitioning(transaction_id, 200), ENSURE_REQUIREMENTS
```

This exchange is introduced by:

```python
.dropDuplicates(["transaction_id"])
```

The repartitioned strategy includes the same transaction-id exchange plus:

```text
Exchange hashpartitioning(event_date, 200), REPARTITION_BY_COL
```

This second exchange is introduced by:

```python
.repartition("event_date")
```

## Event Log Diagnostic

Spark event logging was enabled for one real pipeline run per strategy, using
the same `process_transactions.py` application path and isolated diagnostic
output directories.

The diagnostic runs reproduced the benchmark file counts:

- Baseline diagnostic output: 248 Parquet files
- Repartitioned diagnostic output: 31 Parquet files
- Row-count verification passed for both runs

The event logs identify the Parquet write as SQL execution `4`.

### Baseline Write Stage

- Final write stage: `13`
- Completed write tasks: 8 result tasks
- Write task partition IDs: `0` through `7`
- Output records written by the stage: 100,350

The baseline filesystem output shows:

- 31 `event_date=...` directories
- each date directory contains files from all 8 write task partition IDs
- each write task partition ID appears under all 31 date directories

Therefore, the baseline file count is:

```text
8 write tasks * 31 event_date partitions = 248 Parquet files
```

### Repartitioned Write Stage

- Final write stage: `16`
- Completed write tasks: 9 result tasks
- Write task partition IDs: `0` through `8`
- Output records written by the stage: 100,350

The repartitioned filesystem output shows:

- 31 `event_date=...` directories
- each date directory contains exactly 1 Parquet file
- the 31 dates are distributed across the 9 write task partition IDs

Therefore, the repartitioned file count is:

```text
31 event_date partitions * 1 writer-touch per date = 31 Parquet files
```

## Conclusion

The baseline write produces 248 files because the data entering the partitioned
Parquet write is not clustered by `event_date`. After deduplication, the data is
partitioned by `transaction_id`, and each of the 8 final write tasks contains
rows for all 31 dates. Spark writes one file per task per touched partition
directory.

The repartitioned write produces 31 files because `.repartition("event_date")`
clusters rows by `event_date` before the partitioned write. Each date is touched
by exactly one final write task, so Spark writes one file per date.

This preserves row counts and transformation semantics while improving the
physical file layout. In the verified benchmark, the improved layout also
reduced output bytes and produced a modest median runtime improvement.
