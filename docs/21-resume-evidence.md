# Resume Evidence

## Version 1.2 evidence

Repository-verifiable implementation includes a Docker/Redpanda local configuration, deterministic producer, bounded Spark Structured Streaming consumer, optional idempotent BigQuery `MERGE` loader, two opt-in dbt models with eight tests, observation and alert utilities, a separate monitoring DAG, and three GitHub Actions workflows. Runtime claims must be limited to checks actually recorded in a final verification report.

Do not claim production streaming, production monitoring/alerting, enterprise CI/CD, Kubernetes, managed Airflow, real-time fraud detection, or measured streaming throughput.

## Version 1.3 evidence

Repository-verifiable implementation adds three-topic Redpanda initialization, optional Spark DLQ/risk publication, checkpoint restart verification, a Prometheus JSON-metrics exporter, Alertmanager local webhook, three provisioned Grafana dashboards, nine Prometheus alert rules, a third opt-in streaming dbt mart, and a manual container integration workflow. Runtime wording remains conditional on retained successful Docker or GitHub workflow evidence.

## Version 1.4 evidence

Repository-verifiable implementation adds disabled-by-default Terraform for Pub/Sub, three BigQuery streaming tables, a Dataflow worker identity/IAM, optional Monitoring policies and an optional project-scoped budget; a guarded Pub/Sub producer; an Apache Beam validation/quarantine pipeline; a fourth streaming dbt model; cost preflight, stop, inventory, and cleanup scripts; and a manual-only GCP workflow.

Local evidence may support wording such as “implemented and locally validated a cost-controlled GCP streaming deployment mode.” Until a live run, cleanup, BigQuery validation, and streaming dbt execution are retained as evidence, do not say “deployed,” “operated,” “processed live events on Dataflow,” or quote GCP runtime/cost results.

Use a claim only when the cited artifact or command result supports it.

| Claim | Evidence |
|---|---|
| 100,350 rows, 31 dates, 3,261 fraud rows | BigQuery/local validators and project contracts |
| 248 to 31 files; 87.5% reduction | `batch_100k_strategy_comparison.csv` and investigation report |
| 7.95s to 7.51s; 5.5% improvement | same benchmark evidence |
| 7.86 MB to 5.99 MB; 23.8% reduction | same benchmark evidence |
| 15 dbt models and complete test suite | dbt run/test command output |
| Incremental rerun plus one late insert/correction, zero duplicates | `incremental_merge_evidence.csv` |
| 1M, 5M, 10M local synthetic reconciliation | `scale_test_results.csv` and report |

Do not claim production throughput, distributed-cluster scale, SLA improvement, cost savings, or business revenue impact. Do not describe local scale results as daily production volume.

Also do not claim that a budget guarantees a cap, that Dataflow is bounded/exactly-once end to end, or that the v1.4 GCP path is production-ready or live-verified.

See `docs/resume/verified-project-bullets.md` for role-specific wording and `cognizant-experience-audit.md` for professional-experience boundaries.
