import json

import pytest

from streaming import load_streaming_to_bigquery as loader


def _silver_files(tmp_path):
    silver = tmp_path / "silver"
    for batch in (0, 1):
        path = silver / f"batch_id={batch}" / "part.parquet"
        path.parent.mkdir(parents=True)
        path.write_bytes(b"fixture")
    return silver


def test_loader_replaces_stage_then_appends_and_merges(monkeypatch, tmp_path):
    commands = []

    def fake_run(command, capture_output=False):
        commands.append(command)
        if capture_output:
            return json.dumps([{"row_count": "2", "unique_ids": "2"}])
        return ""

    monkeypatch.setattr(loader, "run_command", fake_run)
    loader.load_streaming_events(_silver_files(tmp_path))

    loads = [command for command in commands if "load" in command]
    assert len(loads) == 2
    assert "--replace" in loads[0]
    assert "--replace" not in loads[1]
    merge_sql = next(command[-1] for command in commands if "MERGE" in command[-1])
    assert "ROW_NUMBER() OVER" in merge_sql
    assert "ON target.transaction_id = source.transaction_id" in merge_sql


def test_loader_fails_if_target_uniqueness_check_fails(monkeypatch, tmp_path):
    def fake_run(command, capture_output=False):
        if capture_output:
            return json.dumps([{"row_count": "2", "unique_ids": "1"}])
        return ""

    monkeypatch.setattr(loader, "run_command", fake_run)
    with pytest.raises(RuntimeError, match="contains duplicates"):
        loader.load_streaming_events(_silver_files(tmp_path))


def test_bigquery_status_file_is_machine_readable(tmp_path):
    output = tmp_path / "load-status.json"
    loader.write_status(output, False, "intentional test failure")
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["success"] is False
    assert payload["details"] == "intentional test failure"
    assert payload["observation_timestamp"]
