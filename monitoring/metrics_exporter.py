"""Expose generated streaming, warehouse-load, and dbt metrics to Prometheus."""

import json
import os
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


DEFAULT_METRICS_ROOT = Path("data/streaming/metrics/transaction_events")
DEFAULT_BIGQUERY_STATUS = Path("data/streaming/load-status/bigquery.json")
DEFAULT_DBT_RESULTS = Path("dbt/risk_analytics/target/run_results.json")


def _timestamp(value: object) -> float:
    if not isinstance(value, str) or not value:
        return 0.0
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def _load_json(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_batches(metrics_root: Path) -> list[dict]:
    return [
        payload
        for path in sorted(metrics_root.glob("batch_id=*.json"))
        if (payload := _load_json(path))
    ]


def collect_metric_values(
    metrics_root: Path,
    bigquery_status: Path,
    dbt_results: Path,
) -> dict[str, float]:
    batches = _load_batches(metrics_root)
    values: dict[str, float] = {
        "streaming_batch_metrics_available": float(bool(batches)),
        "streaming_batches_total": float(len(batches)),
        "streaming_input_events_total": float(
            sum(int(batch.get("input_count", 0)) for batch in batches)
        ),
        "streaming_clean_events_total": float(
            sum(int(batch.get("clean_count", 0)) for batch in batches)
        ),
        "streaming_invalid_events_total": float(
            sum(int(batch.get("invalid_count", 0)) for batch in batches)
        ),
        "streaming_duplicate_events_total": float(
            sum(int(batch.get("duplicate_count", 0)) for batch in batches)
        ),
        "streaming_late_events_total": float(
            sum(int(batch.get("late_count", 0)) for batch in batches)
        ),
        "streaming_schema_drift_events_total": float(
            sum(int(batch.get("schema_drift_count", 0)) for batch in batches)
        ),
        "streaming_dlq_events_total": float(
            sum(int(batch.get("dlq_count", 0)) for batch in batches)
        ),
        "streaming_risk_alert_events_total": float(
            sum(int(batch.get("risk_alert_count", 0)) for batch in batches)
        ),
        "streaming_reconciliation_failures_total": float(
            sum(not bool(batch.get("reconciliation", False)) for batch in batches)
        ),
        "streaming_last_batch_timestamp_seconds": max(
            (_timestamp(batch.get("processing_timestamp")) for batch in batches),
            default=0.0,
        ),
        "streaming_last_batch_duration_seconds": float(
            batches[-1].get("processing_duration_seconds", 0) if batches else 0
        ),
    }
    inputs = values["streaming_input_events_total"]
    values["streaming_invalid_record_ratio"] = (
        values["streaming_invalid_events_total"] / inputs if inputs else 0.0
    )

    load_status = _load_json(bigquery_status)
    values["streaming_bigquery_load_status_available"] = float(bool(load_status))
    values["streaming_bigquery_load_success"] = float(
        load_status.get("success", False)
    )

    dbt_payload = _load_json(dbt_results)
    dbt_rows = dbt_payload.get("results", []) if dbt_payload else []
    streaming_rows = [
        row
        for row in dbt_rows
        if "streaming" in str(row.get("unique_id", "")).lower()
        or "streaming" in str(row.get("node", {}).get("name", "")).lower()
    ]
    values["streaming_dbt_test_results_available"] = float(bool(streaming_rows))
    values["streaming_dbt_test_failures"] = float(
        sum(row.get("status") in {"fail", "error"} for row in streaming_rows)
    )
    return values


def render_prometheus(values: dict[str, float]) -> str:
    descriptions = {
        "streaming_batch_metrics_available": "Whether any streaming batch metrics exist.",
        "streaming_batches_total": "Number of recorded Spark micro-batches.",
        "streaming_input_events_total": "Input events observed by Spark.",
        "streaming_clean_events_total": "Clean deduplicated streaming events.",
        "streaming_invalid_events_total": "Invalid streaming events.",
        "streaming_duplicate_events_total": "Accounted duplicate streaming events.",
        "streaming_late_events_total": "Late clean streaming events.",
        "streaming_schema_drift_events_total": "Events with unexpected fields.",
        "streaming_dlq_events_total": "Invalid events published to the DLQ.",
        "streaming_risk_alert_events_total": "High-value events published as risk alerts.",
        "streaming_reconciliation_failures_total": "Micro-batches that failed reconciliation.",
        "streaming_last_batch_timestamp_seconds": "UTC completion time of the latest batch.",
        "streaming_last_batch_duration_seconds": "Duration of the latest batch.",
        "streaming_invalid_record_ratio": "Invalid events divided by input events.",
        "streaming_bigquery_load_status_available": "Whether BigQuery load status exists.",
        "streaming_bigquery_load_success": "Whether the latest optional BigQuery load passed.",
        "streaming_dbt_test_results_available": "Whether streaming dbt results exist.",
        "streaming_dbt_test_failures": "Failed or errored streaming dbt nodes.",
    }
    lines: list[str] = []
    for name in sorted(values):
        lines.extend(
            [
                f"# HELP {name} {descriptions[name]}",
                f"# TYPE {name} gauge",
                f"{name} {values[name]:.12g}",
            ]
        )
    return "\n".join(lines) + "\n"


class MetricsHandler(BaseHTTPRequestHandler):
    metrics_root = DEFAULT_METRICS_ROOT
    bigquery_status = DEFAULT_BIGQUERY_STATUS
    dbt_results = DEFAULT_DBT_RESULTS

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/health":
            body = b"ok\n"
            status = 200
            content_type = "text/plain"
        elif self.path == "/metrics":
            body = render_prometheus(
                collect_metric_values(
                    self.metrics_root, self.bigquery_status, self.dbt_results
                )
            ).encode("utf-8")
            status = 200
            content_type = "text/plain; version=0.0.4"
        else:
            body = b"not found\n"
            status = 404
            content_type = "text/plain"
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        return


def main() -> None:
    MetricsHandler.metrics_root = Path(
        os.environ.get("STREAMING_METRICS_ROOT", str(DEFAULT_METRICS_ROOT))
    )
    MetricsHandler.bigquery_status = Path(
        os.environ.get("STREAMING_BIGQUERY_STATUS", str(DEFAULT_BIGQUERY_STATUS))
    )
    MetricsHandler.dbt_results = Path(
        os.environ.get("DBT_RUN_RESULTS", str(DEFAULT_DBT_RESULTS))
    )
    host = os.environ.get("METRICS_EXPORTER_HOST", "127.0.0.1")
    port = int(os.environ.get("METRICS_EXPORTER_PORT", "9108"))
    print(f"METRICS EXPORTER LISTENING: http://{host}:{port}/metrics")
    ThreadingHTTPServer((host, port), MetricsHandler).serve_forever()


if __name__ == "__main__":
    main()
