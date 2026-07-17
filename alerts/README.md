# Alerting

Observations with `WARN` or `FAIL` status at or above `ALERT_MIN_SEVERITY` become alerts. Console and local JSON are always available; Slack is optional through `SLACK_WEBHOOK_URL` and its absence is not an error. The webhook is never stored in this repository.

```bash
python alerts/alert_manager.py --observations observability/results/latest.json
python alerts/alert_manager.py --force-demo-alert
```

The forced mode is explicitly labeled as a demo and writes generated output under `alerts/results/`.
