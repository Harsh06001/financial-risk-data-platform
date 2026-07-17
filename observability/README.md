# Data Observability

`collect_metrics.py` emits the same observation contract for tracked local evidence, generated streaming batch metrics, dbt artifacts, and an explicitly selected live BigQuery check. The default `all` mode is local-first and does not access GCP; `--component bigquery` is the opt-in cloud mode.

Every record contains timestamp, pipeline, run, table, metric, value, bounds, status (`PASS`, `WARN`, `FAIL`), severity (`INFO`, `WARNING`, `CRITICAL`), and details. Missing generated streaming/dbt artifacts produce an honest warning rather than a fabricated pass.

```bash
python observability/collect_metrics.py
python observability/collect_metrics.py --component bigquery
```

Results are generated under `observability/results/` and ignored except for `.gitkeep`.
