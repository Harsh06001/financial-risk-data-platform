# Local Streaming Monitoring

The v1.3 Compose stack scrapes Redpanda's low-cardinality `/public_metrics` endpoint and the project JSON metrics exporter. Prometheus evaluates local demo alert rules, Alertmanager routes them to `alert-webhook`, and that receiver prints alerts, writes ignored JSON under `alerts/results/`, and optionally calls `SLACK_WEBHOOK_URL`.

Local endpoints:

| Service | URL |
|---|---|
| Redpanda Console | <http://localhost:8080> |
| Prometheus | <http://localhost:9090> |
| Alertmanager | <http://localhost:9093> |
| Grafana | <http://localhost:3000> |
| Application metrics | <http://localhost:9108/metrics> |

Grafana provisions three dashboards automatically: Kafka and Redpanda Health, Spark Streaming Health, and Streaming Data Quality and Risk. The local Grafana default is `admin`/`admin`; change it through `.env` outside a disposable demo.

The exporter reports generated Spark micro-batch counts, duration, freshness, reconciliation, invalid/duplicate/late/schema-drift/DLQ/risk counts, optional BigQuery load status, and streaming dbt failures. Missing optional cloud/dbt artifacts are explicitly exported as unavailable and do not become fabricated passes.

This stack is a single-host portfolio monitoring environment, not a production observability deployment or pager service.
