# Local Synthetic Scale Test Report

These runs are local synthetic scale tests, not distributed-cluster or production traffic claims.
They are separate from the tracked 100,350-row Spark write-strategy benchmark.

| Requested | Input | Output | Dates | Runtime (s) | Rows/s | Files | Bytes | Status |
|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 1000000 | 1000000 | 1000000 | 31 | 14.7662 | 67722.09 | 31 | 52784849 | PASSED |
| 5000000 | 5000000 | 5000000 | 31 | 29.4523 | 169766.16 | 31 | 243147767 | PASSED |
| 10000000 | 10000000 | 10000000 | 31 | 45.4968 | 219795.88 | 31 | 470591204 | PASSED |

Highest successfully completed scale: 10000000 rows.

## Environment

- spark_master: local[*]
- spark_sql_session_timezone: UTC
- spark_version: 4.1.2
- python_version: 3.14.5
- java_version: openjdk version "17.0.19" 2026-04-21
- cpu_count: 8
- platform: macOS-26.2-arm64-arm-64bit-Mach-O
- write_strategy: repartitioned
- seed: 202611
