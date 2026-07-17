VENV ?= .venv-v12
PYTHON ?= $(VENV)/bin/python
DBT ?= $(VENV)/bin/dbt
COMPOSE ?= docker compose
KAFKA_PACKAGE ?= org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2

.PHONY: setup test docker-up docker-down stream-up monitoring-up stream-produce stream-process stream-validate stream-restart-snapshot stream-restart-verify observe alert-demo dbt-run dbt-test validate ci-local

setup:
	python3.11 -m venv $(VENV)
	$(PYTHON) -m pip install -r requirements.txt -r requirements-dbt.txt -r requirements-streaming.txt -r requirements-dev.txt

test:
	$(PYTHON) -m pytest tests -q

docker-up:
	$(COMPOSE) up -d

docker-down:
	$(COMPOSE) down

stream-up:
	$(COMPOSE) up -d redpanda topic-init

monitoring-up:
	$(COMPOSE) up -d redpanda topic-init kafka-ui metrics-exporter alert-webhook alertmanager prometheus grafana

stream-produce:
	$(COMPOSE) run --rm app python -m streaming.producer.produce_transaction_events --bootstrap-servers redpanda:29092 --count 30 --rate 10 --seed 202613 --invalid-every 10 --duplicate-every 12 --late-every 15

stream-process:
	$(COMPOSE) run --rm app spark-submit --packages $(KAFKA_PACKAGE) streaming/spark/process_transaction_stream.py --bootstrap-servers redpanda:29092 --available-now

stream-validate:
	$(COMPOSE) run --rm app python streaming/spark/validate_stream_output.py

stream-restart-snapshot:
	$(COMPOSE) run --rm app python -m streaming.spark.verify_checkpoint_restart --metrics-root data/streaming/metrics/transaction_events --snapshot data/streaming/checkpoint-snapshot.json --write-snapshot

stream-restart-verify:
	$(COMPOSE) run --rm app python -m streaming.spark.verify_checkpoint_restart --metrics-root data/streaming/metrics/transaction_events --snapshot data/streaming/checkpoint-snapshot.json

observe:
	$(PYTHON) observability/collect_metrics.py

alert-demo:
	$(PYTHON) alerts/alert_manager.py --force-demo-alert

dbt-run:
	$(DBT) run --project-dir dbt/risk_analytics --profiles-dir dbt/risk_analytics

dbt-test:
	$(DBT) test --project-dir dbt/risk_analytics --profiles-dir dbt/risk_analytics

validate:
	$(PYTHON) scripts/validate_repository.py

ci-local: validate test
	terraform -chdir=infrastructure/terraform fmt -check -recursive
