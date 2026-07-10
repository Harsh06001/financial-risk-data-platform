# dbt Modeling and Tests

The dbt graph contains three layers:

- `staging`: six source-aligned views.
- `core`: incremental transaction fact plus three current-state dimensions.
- `marts`: five risk analytics tables.

Sources point to native BigQuery tables. Jinja `source()` and `ref()` express lineage. The fact uses `materialized='incremental'`, BigQuery `merge`, unique key `transaction_id`, date partitioning, and schema-change failure. Core dimensions and marts use table materialization; staging uses views.

Tests cover identifiers, grains, nulls, relationship integrity, risk-reason values and logic, fraud-rate bounds, daily/segment totals, daily mart reconciliation, and fact/source count reconciliation. Tests return invalid rows; zero rows means pass.

Commands:

```bash
.venv-dbt2/bin/dbt debug --project-dir dbt/risk_analytics --profiles-dir dbt/risk_analytics
.venv-dbt2/bin/dbt run --project-dir dbt/risk_analytics --profiles-dir dbt/risk_analytics
.venv-dbt2/bin/dbt test --project-dir dbt/risk_analytics --profiles-dir dbt/risk_analytics
```

The repository OAuth profile contains no key path or credential. Project, dataset, and location can be supplied by environment variables.
