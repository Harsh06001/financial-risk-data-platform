# Alerting

Observations with `WARN` or `FAIL` status at or above `ALERT_MIN_SEVERITY` become alerts. Console and local JSON are always available; Slack is optional through `SLACK_WEBHOOK_URL` and its absence is not an error. The webhook is never stored in this repository.

```bash
python alerts/alert_manager.py --observations observability/results/latest.json
python alerts/alert_manager.py --force-demo-alert
```

The forced mode is explicitly labeled as a demo and writes generated output under `alerts/results/`.

Version 1.3 also routes Prometheus rules through Alertmanager to `monitoring.alert_webhook`. That receiver preserves the same console/local-JSON/optional-Slack behavior. Missing Slack configuration is safe; an explicitly configured but failing webhook remains visible for retry.
