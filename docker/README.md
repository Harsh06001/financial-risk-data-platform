# Docker Local Environment

Docker Compose contains the application image, one Redpanda broker, Redpanda Console, Prometheus, Grafana, Alertmanager, and lightweight project exporter/webhook services. Airflow remains a separately documented local runtime because packaging its scheduler, metadata database, and webserver would make this demo materially heavier.

Copy `.env.example` to `.env` and adjust non-secret values. Never place service-account JSON in the build context. The deliberately lean application image does not include the Google Cloud CLI; run `streaming/load_streaming_to_bigquery.py` on a host where `bq` and ADC are configured. A containerized cloud loader is not part of this local demo.

Common commands:

```bash
make stream-up
make monitoring-up
make stream-produce
make stream-process
make stream-validate
make observe
make alert-demo
make docker-down
```

Host endpoints are Kafka `localhost:19092`, Redpanda Admin `localhost:19644`, Console `localhost:8080`, Prometheus `localhost:9090`, Alertmanager `localhost:9093`, Grafana `localhost:3000`, and application metrics `localhost:9108`. Containers use `redpanda:29092` for Kafka.

`docker compose down` stops services but deliberately does not remove the named Redpanda volume. Use a fresh Compose project name when an isolated broker state is required.
