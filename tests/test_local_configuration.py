from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_compose_has_narrow_local_services_and_persistent_broker_volume():
    compose = yaml.safe_load(
        (PROJECT_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    )
    assert set(compose["services"]) == {
        "redpanda",
        "topic-init",
        "app",
        "kafka-ui",
        "metrics-exporter",
        "alert-webhook",
        "prometheus",
        "alertmanager",
        "grafana",
    }
    assert compose["services"]["redpanda"]["ports"] == ["19092:19092", "19644:9644"]
    assert "redpanda-data" in compose["volumes"]
    assert compose["services"]["prometheus"]["ports"] == ["9090:9090"]
    assert compose["services"]["grafana"]["ports"] == ["3000:3000"]


def test_topic_initializer_is_idempotent_and_covers_all_topics():
    script = (PROJECT_ROOT / "docker/scripts/init-topics.sh").read_text(
        encoding="utf-8"
    )
    assert "transaction-events" in script
    assert "transaction-events-dlq" in script
    assert "streaming-risk-alerts" in script
    assert "rpk topic describe" in script
    assert "|| true" not in script


def test_examples_do_not_embed_credential_values():
    environment = (PROJECT_ROOT / ".env.example").read_text(encoding="utf-8")
    dockerfile = (PROJECT_ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "PRIVATE KEY" not in environment
    assert "credentials.json" not in dockerfile
    assert "COPY . ." in dockerfile
    assert "credentials" in (PROJECT_ROOT / ".dockerignore").read_text(encoding="utf-8")
