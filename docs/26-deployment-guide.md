# Deployment Guide

```mermaid
flowchart LR
    subgraph Local portfolio mode
      M[Make + virtualenv] --> R[Redpanda + app container]
      R --> F[(Local generated files)]
    end
    subgraph Cloud-enabled mode
      A[Authenticated host / manual CI] --> G[GCS + BigQuery]
      G --> D[dbt]
      A --> AF[Optional local Airflow scheduler]
    end
    F -. explicit host loader .-> G
```

Local mode requires Python 3.11, Java 17 for Spark, Docker Compose for Redpanda, and `make setup`. Copy `.env.example` only for non-secret configuration. Run `make stream-up`, producer, consumer, validator, observations, and alerts in order.

Cloud-enabled mode assumes existing Terraform-provisioned v1.1 resources, ADC or manual-workflow credentials, `bq`, and appropriate least-privilege access. Run the optional streaming loader only after local silver validation; then enable the two streaming dbt models explicitly. Terraform remains a separate plan/review/apply lifecycle—this upgrade does not automatically apply infrastructure.

Airflow can load both DAGs when `AIRFLOW_HOME` and dependencies are configured locally. `AIRFLOW_PROJECT_PYTHON` can point monitoring tasks at the v1.2 environment (the default is `.venv-v12/bin/python`). It is orchestration demonstration code, not a managed Composer deployment.

Version 1.3 local mode starts Redpanda Console, Prometheus, Alertmanager, Grafana, the application exporter, and local alert webhook with `make docker-up`. Cloud-enabled loading remains host-side and explicit. The monitoring services contain local defaults and persistent development volumes; they are not hardened for external exposure.

Version 1.4 adds a second, disabled-by-default cloud streaming mode. It requires a reviewed Terraform plan, explicit cost acknowledgement, one-worker limits, and immediate post-run cancellation/inventory. See [GCP streaming deployment](28-gcp-streaming-deployment.md) and [cost controls](29-gcp-cost-controls-and-cleanup.md). It is not part of normal CI or the default batch deployment.
