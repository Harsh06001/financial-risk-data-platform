# Version 1.2 Operations Runbook

## Normal local demonstration

1. `make setup`
2. `make stream-up`
3. `make stream-produce`
4. `make stream-process`
5. `make stream-validate`
6. `make observe`
7. `python alerts/alert_manager.py --observations observability/results/latest.json`
8. `make docker-down`

Use a new producer seed for a distinct deterministic input set. A preserved Spark checkpoint consumes only unseen Kafka offsets. Do not delete checkpoints as a normal rerun mechanism. If a deliberately isolated reset is needed, use a new `--data-root` and Compose project name rather than broad deletion.

## Triage

- Redpanda unhealthy: inspect broker health and port `19092`; do not bypass topic-init errors.
- Quarantine increase: inspect raw JSON and `validation_error`; correct the producer/source contract before loading.
- Reconciliation failure: stop the load; input must equal clean + invalid + duplicate.
- BigQuery duplicate failure: retain the stage table and inspect dedup ordering; do not delete the target.
- dbt failure: keep streaming models disabled unless the optional source table is present.
- Missing Slack: console/JSON remain successful by design.
- Monitoring DAG BigQuery task failure: verify ADC, `bq`, project, dataset, and location.

## Recovery and rollback

Generated outputs are audit-like by batch directory and BigQuery loads are staged plus `MERGE`. Code rollback is a normal Git change; cloud resource changes require reviewed Terraform plans. Never use broad bucket/dataset deletion for recovery.

Production improvements include multi-broker managed Kafka, schema registry, cross-batch stateful deduplication policy, transactional object/table publication, managed Spark, durable metrics tables, paging/routing, secret manager integration, workload identity federation, deployment environments, and measured SLAs.

For the v1.3 three-topic and Prometheus/Grafana/Alertmanager deployment, continue with the [v1.3 runbook](29-v1-3-demo-runbook.md).
