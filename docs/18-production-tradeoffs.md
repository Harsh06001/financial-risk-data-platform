# Production Trade-offs

The repository optimizes for inspectability on one machine and a development GCP project.

At roughly 1 GB, local PySpark, directory Parquet, CLI orchestration, and full feature refreshes are understandable and sufficient. At 1 TB, use managed Spark, distributed source files, centralized metadata, scheduled compaction, partition-aware feature recomputation, service accounts, remote Terraform state, monitoring, and cost budgets. At 100 TB, add table formats/catalog transactions, generation-based publishing, robust backfills, workload isolation, autoscaling, data lineage, SLA monitoring, and formal governance.

Known v1.1 limitations:

- full warehouse tables publish sequentially, not atomically;
- GCS verification checks names, not independent checksums;
- development Terraform settings are intentionally permissive;
- dimensions are Type 1/current state because history is absent;
- feature aggregates still full-refresh after transaction MERGE;
- incremental selection scans the available raw CSV glob before filtering;
- Airflow runtime validation is not available locally;
- synthetic local results do not predict cluster or production performance.

Kafka, streaming, Kubernetes, Dataflow, and machine learning are intentionally absent because they do not solve a demonstrated v1.1 requirement.
