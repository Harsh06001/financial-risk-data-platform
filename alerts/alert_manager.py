"""Emit evaluated alerts to console, JSON, and an optional Slack webhook."""

import argparse
import json
import os
import sys
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from alerts.evaluate_alerts import evaluate_observations, load_observations


DEFAULT_OUTPUT = PROJECT_ROOT / "alerts/results/latest.json"


def send_slack(alerts: list[dict], webhook_url: str) -> None:
    text = "\n".join(
        f"[{item['severity']}] {item['pipeline_name']}.{item['metric_name']}: "
        f"{item['metric_value']} ({item['status']})"
        for item in alerts
    )
    request = urllib.request.Request(
        webhook_url,
        data=json.dumps({"text": text}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        if response.status >= 300:
            raise RuntimeError(f"Slack returned HTTP {response.status}")


def emit(alerts: list[dict], output: Path, webhook_url: str | None = None) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(alerts, indent=2) + "\n", encoding="utf-8")
    if not alerts:
        print("NO ALERTS TO EMIT")
    for item in alerts:
        print(
            f"ALERT [{item['severity']}] {item['pipeline_name']}."
            f"{item['table_name']}.{item['metric_name']}={item['metric_value']}"
        )
    if webhook_url and alerts:
        send_slack(alerts, webhook_url)
        print("SLACK ALERTS SENT")
    elif alerts:
        print("SLACK_WEBHOOK_URL is unset; console and JSON channels completed.")
    print(f"ALERT FILE WRITTEN: {output}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Emit data quality alerts.")
    parser.add_argument("--observations", type=Path, action="append")
    parser.add_argument("--alerts-input", type=Path)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--force-demo-alert", action="store_true")
    args = parser.parse_args()

    if args.alerts_input:
        alerts = json.loads(args.alerts_input.read_text(encoding="utf-8"))
    else:
        observations = load_observations(args.observations or [])
        if args.force_demo_alert:
            observations.append(
                {
                    "pipeline_name": "forced_local_demo",
                    "table_name": "demo_table",
                    "metric_name": "row_count",
                    "metric_value": 0,
                    "status": "FAIL",
                    "severity": "CRITICAL",
                    "details": "Intentional local alert test; not a real pipeline failure.",
                }
            )
        alerts = evaluate_observations(
            observations, os.environ.get("ALERT_MIN_SEVERITY", "WARNING")
        )
    emit(alerts, args.output, os.environ.get("SLACK_WEBHOOK_URL"))


if __name__ == "__main__":
    main()
