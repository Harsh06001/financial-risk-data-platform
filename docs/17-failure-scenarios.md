# Failure Scenarios

Version 1.2 adds explicit failure surfaces for broker health, Kafka connector resolution, malformed events, checkpoint access, micro-batch reconciliation, host-side BigQuery staging/`MERGE`, missing optional dbt sources, observation bounds, and optional Slack delivery. Missing generated metrics are warnings; reconciliation or contract breaches are failures. See the [operations runbook](27-v1-2-operations-runbook.md).

| Failure | Detection | Result |
|---|---|---|
| Invalid/missing raw fields | CSV validator and Spark schema/filters | pipeline exits |
| Requested date invalid or absent | argparse/date row count | no partition write |
| Duplicate transaction ID | processed/stage/dbt uniqueness checks | load/merge blocked |
| Empty/incomplete sync source | preflight inventory contracts | deletion refused |
| Wrong GCS destination | exact URI guard | rsync refused |
| Unrelated partition changes | before/after root inventory | incremental sync fails |
| BigQuery count/grain mismatch | warehouse validator | dbt not run by pipeline |
| Broken fact reference | dbt relationship tests | test failure |
| Partial subprocess failure | `check=True` | later stages stop |
| Scale timeout/JVM/resource error | captured exception and FAILED row | evidence records failure |

Failure propagation is deliberately simple and visible. There is no cross-table atomic publish, automatic rollback, or dead-letter queue in v1.1. Operators rerun safe stages after correcting the cause.

The scale timezone failure illustrates the policy: keep the assertion, fix the implementation, rerun, and publish only the successful evidence with its configuration.
