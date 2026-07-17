import json

import pytest

from gcp_streaming.producer.publish_transaction_events import (
    parse_arguments,
    run,
    validate_publish_request,
)


def test_dry_run_is_deterministic_and_writes_jsonl(tmp_path):
    first_output = tmp_path / "first.jsonl"
    second_output = tmp_path / "second.jsonl"
    first_args = parse_arguments(
        ["--dry-run", "--count", "8", "--seed", "77", "--output", str(first_output)]
    )
    second_args = parse_arguments(
        ["--dry-run", "--count", "8", "--seed", "77", "--output", str(second_output)]
    )
    assert run(first_args) == run(second_args)
    assert first_output.read_text(encoding="utf-8") == second_output.read_text(encoding="utf-8")
    assert len(first_output.read_text(encoding="utf-8").splitlines()) == 8
    assert json.loads(first_output.read_text(encoding="utf-8").splitlines()[0])["transaction_id"]


def test_large_demo_is_refused_without_override():
    with pytest.raises(ValueError, match="--allow-large-demo"):
        validate_publish_request(10_001, False, True, False)
    validate_publish_request(10_001, True, True, False)


def test_live_publish_requires_cost_acknowledgement():
    with pytest.raises(ValueError, match="acknowledge-cost-risk"):
        validate_publish_request(1000, False, False, False)
    validate_publish_request(1000, False, False, True)
