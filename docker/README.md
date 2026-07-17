# Docker Local Environment

Docker Compose intentionally contains only the application image and one Redpanda broker. Airflow remains a separately documented local runtime because packaging its scheduler, metadata database, and webserver would make this demo materially heavier.

Copy `.env.example` to `.env` and adjust non-secret values. Never place service-account JSON in the build context. The deliberately lean application image does not include the Google Cloud CLI; run `streaming/load_streaming_to_bigquery.py` on a host where `bq` and ADC are configured. A containerized cloud loader is not part of this local demo.

Common commands:

```bash
make stream-up
make stream-produce
make stream-process
make stream-validate
make observe
make alert-demo
make docker-down
```

`docker compose down` stops services but deliberately does not remove the named Redpanda volume. Use a fresh Compose project name when an isolated broker state is required.
