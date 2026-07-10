# Spark Execution Internals

Spark builds logical plans for DataFrame transformations and executes only when an action is requested. Schema projection and filters can be pushed toward the scan. `dropDuplicates(transaction_id)` requires a shuffle so equal IDs meet. Aggregations group data by their keys and also shuffle.

The original baseline write inherited transaction-ID partitioning after deduplication. Eight writers touched every one of 31 date directories, creating 248 files. The optimized path adds `repartition(event_date)`, clustering each date before the partitioned write and producing 31 files.

Verified evidence is in `benchmarks/results/batch_write_file_count_investigation.md` and `batch_100k_strategy_comparison.csv`:

- baseline median 7.95s, 248 files, 7.86 MB;
- optimized median 7.51s, 31 files, 5.99 MB;
- 5.5% runtime, 87.5% file-count, and 23.8% storage reduction.

Local `local[*]` uses eight logical CPUs on the measured machine. These results do not imply cluster performance. At larger scale, inspect Spark UI/event logs, skew, spill, partition counts, executor memory, and shuffle read/write rather than assuming the local strategy generalizes unchanged.
