from gcp_streaming.cost_controls import CostConfig


def test_default_guarded_config_passes_when_acknowledged():
    config = CostConfig(project_id="demo-project", acknowledged=True)
    assert config.demo_event_count == 1000
    assert config.max_demo_minutes == 15
    assert config.num_workers == 1
    assert config.max_workers == 1
    assert config.errors() == []


def test_preflight_rejects_unsafe_runtime_workers_and_missing_ack():
    config = CostConfig(
        project_id="",
        demo_event_count=10_001,
        max_demo_minutes=16,
        num_workers=2,
        max_workers=3,
        acknowledged=False,
    )
    errors = "\n".join(config.errors())
    assert "GCP_PROJECT_ID" in errors
    assert "between 1 and 10000" in errors
    assert "between 1 and 15" in errors
    assert "exactly one worker" in errors
    assert "must be 1 or 2" in errors
    assert "acknowledgement" in errors
