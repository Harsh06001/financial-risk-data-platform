# Version 1.3 Demo and Troubleshooting Runbook

## Startup and health

Run `make docker-up`, then inspect `docker compose ps`. `topic-init` should exit successfully after describing or creating all three topics. Open Redpanda Console and confirm those topic names before producing data.

The first Spark run creates bronze, silver, quarantine, metrics, and checkpoint paths. Validation requires `input = clean + invalid + duplicate`, limits the invalid ratio, and ensures DLQ count never exceeds invalid count.

## Common failures

| Symptom | Check | Safe response |
|---|---|---|
| Broker unhealthy | Redpanda logs and port `19092` | Correct port/resource configuration; do not bypass topic-init |
| Connector resolution failure | Maven access and Spark/Scala coordinate | Retain the checkpoint and retry after dependency access is restored |
| Quarantine or DLQ increase | Raw JSON and schema-drift count | Correct the source contract before BigQuery loading |
| Restart changes snapshot | Checkpoint path and starting offsets | Stop; do not treat the run as idempotent until explained |
| Prometheus target down | `/public_metrics`, exporter `/health` | Fix network/service health before trusting dashboards |
| Grafana empty | Prometheus datasource and generated batch metrics | Run the bounded stream; missing artifacts are intentionally not fabricated |
| Alert webhook retries | `alert-webhook` health and logs | Console/file works without Slack; correct a configured invalid webhook separately |
| BigQuery load failed | `data/streaming/load-status/bigquery.json` and stage table | Preserve target/stage evidence; fix and rerun the `MERGE` loader |
| Streaming dbt disabled | Source table and `enable_streaming_models` | Keep disabled until the optional source exists |

## Deployment modes

- Local portfolio mode: all containers, generated local Parquet/JSON, no GCP credentials.
- Cloud-enabled mode: authenticated host performs optional BigQuery load and streaming dbt run/test.
- Manual CI mode: `streaming-integration.yml` runs the local container smoke path; `optional-cloud-integration.yml` remains separately credential-gated.

Do not remove persistent volumes or checkpoints as a routine retry. Use a new isolated data root and Compose project name for an intentional clean demonstration.
