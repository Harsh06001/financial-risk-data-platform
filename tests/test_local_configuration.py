from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_compose_has_narrow_local_services_and_persistent_broker_volume():
    compose = yaml.safe_load(
        (PROJECT_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    )
    assert set(compose["services"]) == {"redpanda", "topic-init", "app"}
    assert compose["services"]["redpanda"]["ports"] == ["19092:19092", "19644:9644"]
    assert "redpanda-data" in compose["volumes"]


def test_examples_do_not_embed_credential_values():
    environment = (PROJECT_ROOT / ".env.example").read_text(encoding="utf-8")
    dockerfile = (PROJECT_ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "PRIVATE KEY" not in environment
    assert "credentials.json" not in dockerfile
    assert "COPY . ." in dockerfile
    assert "credentials" in (PROJECT_ROOT / ".dockerignore").read_text(encoding="utf-8")
