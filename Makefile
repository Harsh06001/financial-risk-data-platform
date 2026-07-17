VENV ?= .venv-v12
PYTHON ?= $(VENV)/bin/python
DBT ?= $(VENV)/bin/dbt
COMPOSE ?= docker compose
KAFKA_PACKAGE ?= org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2

.PHONY: setup test docker-up docker-down stream-up monitoring-up stream-produce stream-process stream-validate stream-restart-snapshot stream-restart-verify observe alert-demo dbt-run dbt-test validate ci-local gcp-streaming-preflight gcp-streaming-plan gcp-streaming-producer-dry-run gcp-streaming-demo gcp-streaming-stop gcp-streaming-cleanup gcp-streaming-check-active gcp-streaming-dbt-parse gcp-streaming-dbt-run gcp-streaming-dbt-test

setup:
	python3.11 -m venv $(VENV)
	$(PYTHON) -m pip install -r requirements.txt -r requirements-dbt.txt -r requirements-streaming.txt -r requirements-dev.txt

test:
	$(PYTHON) -m pytest tests gcp_streaming/tests -q

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

gcp-streaming-preflight:
	@echo "COST WARNING: validates live GCP settings only; requires explicit acknowledgement."
	PYTHON_BIN=$(PYTHON) gcp_streaming/scripts/preflight_cost_check.sh

gcp-streaming-plan: gcp-streaming-preflight
	@echo "COST WARNING: planning only; this target never applies Terraform."
	terraform -chdir=infrastructure/terraform plan -var=enable_gcp_streaming=true -var=demo_event_count=$${GCP_STREAMING_DEMO_EVENT_COUNT:-1000} -var=max_demo_minutes=$${GCP_STREAMING_MAX_DEMO_MINUTES:-15} -var=dataflow_num_workers=$${GCP_STREAMING_NUM_WORKERS:-1} -var=dataflow_max_workers=$${GCP_STREAMING_MAX_WORKERS:-1}

gcp-streaming-producer-dry-run:
	$(PYTHON) -m gcp_streaming.producer.publish_transaction_events --dry-run --count $${GCP_STREAMING_DEMO_EVENT_COUNT:-1000} --output /tmp/gcp-streaming-demo.jsonl

gcp-streaming-demo:
	@echo "COST WARNING: this starts a billable Dataflow job and requires ACKNOWLEDGE_GCP_COST_RISK=true."
	PYTHON_BIN=$(PYTHON) gcp_streaming/scripts/run_short_demo.sh

gcp-streaming-stop:
	gcp_streaming/scripts/stop_dataflow_jobs.sh --cancel

gcp-streaming-cleanup:
	gcp_streaming/scripts/cleanup_streaming_demo.sh $${GCP_STREAMING_CLEANUP_ARGS:-}

gcp-streaming-check-active:
	gcp_streaming/scripts/check_active_cost_resources.sh

gcp-streaming-dbt-parse:
	$(DBT) parse --no-partial-parse --project-dir dbt/risk_analytics --profiles-dir dbt/risk_analytics --vars '{enable_gcp_streaming_models: true}'

gcp-streaming-dbt-run:
	@echo "COST WARNING: this queries and creates BigQuery models."
	$(DBT) run --project-dir dbt/risk_analytics --profiles-dir dbt/risk_analytics --vars '{enable_gcp_streaming_models: true}' --select path:models/streaming

gcp-streaming-dbt-test:
	@echo "COST WARNING: this queries BigQuery streaming tables."
	$(DBT) test --project-dir dbt/risk_analytics --profiles-dir dbt/risk_analytics --vars '{enable_gcp_streaming_models: true}' --select path:models/streaming path:tests/test_streaming* path:tests/test_realtime*
