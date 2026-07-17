"""Alertmanager webhook receiver with local file/console and optional Slack output."""

import json
import os
import threading
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from alerts.alert_manager import send_slack


WRITE_LOCK = threading.Lock()


def normalize_alerts(payload: dict) -> list[dict]:
    normalized = []
    for alert in payload.get("alerts", []):
        labels = alert.get("labels", {})
        annotations = alert.get("annotations", {})
        normalized.append(
            {
                "alert_timestamp": datetime.now(timezone.utc).isoformat(),
                "alert_name": labels.get("alertname", "unknown"),
                "severity": str(labels.get("severity", "WARNING")).upper(),
                "status": str(alert.get("status", payload.get("status", "firing"))).upper(),
                "pipeline_name": labels.get("pipeline", "streaming_platform"),
                "table_name": labels.get("table", "streaming_transaction_events"),
                "metric_name": labels.get("metric", labels.get("alertname", "unknown")),
                "metric_value": annotations.get("value", "n/a"),
                "summary": annotations.get("summary", ""),
                "details": annotations.get("description", ""),
            }
        )
    return normalized


def persist_and_emit(payload: dict, output: Path, slack_url: str | None) -> list[dict]:
    alerts = normalize_alerts(payload)
    output.parent.mkdir(parents=True, exist_ok=True)
    with WRITE_LOCK:
        output.write_text(json.dumps(alerts, indent=2) + "\n", encoding="utf-8")
    for alert in alerts:
        print(
            f"ALERTMANAGER [{alert['severity']}] {alert['alert_name']}: "
            f"{alert['summary']}"
        )
    if slack_url and alerts:
        send_slack(alerts, slack_url)
        print("ALERTMANAGER SLACK NOTIFICATION SENT")
    elif alerts:
        print("SLACK_WEBHOOK_URL is unset; console and local JSON completed.")
    return alerts


class AlertWebhookHandler(BaseHTTPRequestHandler):
    output = Path("alerts/results/alertmanager.json")
    slack_url: str | None = None

    def do_GET(self) -> None:  # noqa: N802
        body = b"ok\n" if self.path == "/health" else b"not found\n"
        self.send_response(200 if self.path == "/health" else 404)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/alerts":
            self.send_error(404)
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
            persist_and_emit(payload, self.output, self.slack_url)
            body = b'{"status":"accepted"}\n'
            self.send_response(200)
        except Exception as exc:
            body = json.dumps({"status": "error", "error": str(exc)}).encode("utf-8")
            self.send_response(500)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> None:
    AlertWebhookHandler.output = Path(
        os.environ.get("ALERT_WEBHOOK_OUTPUT", "alerts/results/alertmanager.json")
    )
    AlertWebhookHandler.slack_url = os.environ.get("SLACK_WEBHOOK_URL") or None
    host = os.environ.get("ALERT_WEBHOOK_HOST", "127.0.0.1")
    port = int(os.environ.get("ALERT_WEBHOOK_PORT", "8088"))
    print(f"ALERT WEBHOOK LISTENING: http://{host}:{port}/alerts")
    ThreadingHTTPServer((host, port), AlertWebhookHandler).serve_forever()


if __name__ == "__main__":
    main()
