import json
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_prometheus_alert_contract_contains_required_rules():
    config = yaml.safe_load(
        (PROJECT_ROOT / "monitoring/prometheus/alerts.yml").read_text(encoding="utf-8")
    )
    names = {
        rule["alert"]
        for group in config["groups"]
        for rule in group["rules"]
    }
    assert names == {
        "KafkaBrokerUnavailable",
        "KafkaConsumerLagHigh",
        "NoStreamingBatchMetrics",
        "NoRecentStreamingBatch",
        "StreamingInvalidRecordRateHigh",
        "StreamingDlqEventsDetected",
        "StreamingBigQueryLoadFailed",
        "StreamingDbtTestsFailed",
        "StreamingDataFreshnessMissed",
    }


def test_three_grafana_dashboards_are_valid_and_have_unique_uids():
    dashboard_root = PROJECT_ROOT / "monitoring/grafana/dashboards"
    dashboards = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in sorted(dashboard_root.glob("*.json"))
    ]
    assert len(dashboards) == 3
    assert len({dashboard["uid"] for dashboard in dashboards}) == 3
    assert all(dashboard["panels"] for dashboard in dashboards)
