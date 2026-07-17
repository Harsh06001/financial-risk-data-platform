# Production Trade-offs

The v1.2 single-broker Redpanda, local Spark, JSON observations, optional webhook, local Airflow setup, and manual cloud workflow optimize for a controlled portfolio demonstration. A production design would require managed/replicated services, capacity and failure testing, state-retention policy, schema compatibility management, durable metrics, paging and ownership, secret management, workload identity, deployment promotion, rollback automation, and measured SLOs.

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

Version 1.3 added local Kafka-compatible streaming; version 1.4 adds an optional Pub/Sub/Dataflow deployment definition. Neither changes the canonical batch evidence or establishes production readiness. A production GCP streaming design would additionally require measured capacity and cost baselines, schema-registry/change policy, delivery-semantics analysis, replay/backfill strategy, SLOs, multi-environment promotion, stronger IAM boundaries, remote Terraform state, retention governance, tested disaster recovery, and continuous ownership.

The v1.4 demo deliberately disables autoscaling and limits workers. That is appropriate for cost containment, not throughput. Pub/Sub is unbounded, so manual cancellation and active-job verification remain required. Budgets notify but do not cap spend, and Dataflow/Monitoring pricing can change. Kubernetes and machine learning remain outside scope.
