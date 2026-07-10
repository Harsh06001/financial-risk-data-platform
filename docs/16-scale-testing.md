# Local Synthetic Scale Testing

`benchmarks/run_scale_tests.py` generates deterministic UUID5-based CSV data with seed 202611, processes it with the optimized strategy, validates exact reconciliation, and writes lightweight evidence. Generated data remains under ignored `data/scale-tests/`.

Verified results:

| Requested/input/output | Dates | Processing seconds | Rows/s | Parquet files | Output bytes |
|---:|---:|---:|---:|---:|---:|
| 1,000,000 | 31 | 14.7662 | 67,722.09 | 31 | 52,784,849 |
| 5,000,000 | 31 | 29.4523 | 169,766.16 | 31 | 243,147,767 |
| 10,000,000 | 31 | 45.4968 | 219,795.88 | 31 | 470,591,204 |

Runtime measures only `process_transactions.py`; it excludes generation and validation. Environment: local[*], Spark 4.1.2, Python 3.14.5, Java 17.0.19, eight CPUs, macOS arm64, UTC Spark SQL timezone.

An initial run correctly failed because host-local timezone conversion created 32 dates. The processing application now defaults Spark SQL to UTC (with an explicit override available), the harness records UTC, and all sizes were rerun. This failed attempt is engineering history, not a resume metric.

These are local synthetic results, not production throughput or distributed-cluster evidence. Canonical optimization evidence remains separate.
