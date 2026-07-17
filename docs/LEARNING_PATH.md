# Learning Path

After completing the v1.1 batch, warehouse, incremental, dbt, and Airflow sections, continue with [Docker/local development](22-docker-and-local-development.md), [streaming](23-streaming-pipeline.md), [observability/alerting](24-data-observability-and-alerting.md), [CI/CD](25-ci-cd.md), [deployment](26-deployment-guide.md), and the [operations runbook](27-v1-2-operations-runbook.md).

Then complete the [v1.3 streaming runtime and monitoring guide](28-v1-3-streaming-runtime-and-monitoring.md) and reproduce the [checkpoint and troubleshooting runbook](29-v1-3-demo-runbook.md).

For v1.4, study the opt-in [GCP streaming deployment](28-gcp-streaming-deployment.md), read every [cost and cleanup guard](29-gcp-cost-controls-and-cleanup.md), then rehearse the [GCP runbook](30-gcp-streaming-runbook.md) without creating resources. Do not run the live exercise until the Terraform plan, budget, and explicit approval are in place.

Each module should end with an explanation in your own words and one small code or query exercise.

| Module | Read | Run | Be able to answer / exercise |
|---|---|---|---|
| 1 Problem and architecture | 00–01 | inspect README | draw both flows; mark state boundaries |
| 2 Raw data/contracts | 02, generator/validator | generator `--help` | explain schema and grains; add an invalid row locally |
| 3 PySpark ETL | 04 | full local pipeline | trace one row through transform/clean/write |
| 4 Spark internals | 05–06 | benchmark scripts | identify actions/shuffles; compare plans |
| 5 Validation | 07 | processed/feature validators | explain reconciliation versus schema tests |
| 6 Features | 08 | build/validate features | derive fraud rate manually for one group |
| 7 GCS | 09 | inspect sync preflight | explain deletion guards and idempotency |
| 8 BigQuery | 10 | warehouse validator | compare native/external tables |
| 9 Dimensional model | 11 | query fact/dim grains | draw star schema; justify natural keys |
| 10 Incremental/MERGE | 12–13 | incremental and late-data scripts | explain dynamic overwrite and matched/unmatched actions |
| 11 dbt | 14 | dbt debug/run/test | trace source→staging→core→mart; add a safe test |
| 12 Airflow | 15 | parse DAG source | explain retries and failure propagation |
| 13 Scale testing | 16 | inspect evidence CSV | calculate throughput; state what it does not prove |
| 14 Failures/trade-offs | 17–18 | simulate an invalid date | propose 1 TB and 100 TB changes |
| 15 Resume/interview | 20–21 and resume docs | rehearse walkthrough | defend every number and limitation |
| 16 Cost-controlled GCP streaming | 28–30 (v1.4) | producer dry run, tests, Terraform plan | explain unbounded-job risk, quarantine, IAM, stop and cleanup |

Suggested pace is one module per week, but modules 3–5 and 9–11 are worth extra practice because they connect implementation details to common Data Engineer interviews.
