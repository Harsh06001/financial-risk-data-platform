# File and Function Reference

This chapter covers authored runtime files; generated data, dbt targets/logs, Terraform state, and virtual environments are excluded.

| File | Why/caller | Major functions and contracts | Side effects/failures |
|---|---|---|---|
| `data-generator/generate_transactions.py` | deterministic fixtures; scale runner | `generate_transaction(rng,index,seed,start,event_dates)->dict`; `generate_dataset(...)->Path` | writes CSV; rejects nonpositive rows/dates |
| `data-generator/validate_transactions.py` | raw runner | schema/value/ID validation | reads CSV; false on contract error |
| `batch-processing/process_transactions.py` | full/incremental/scale runners | create session; read schema; transform; clean; partition write; parse date | Spark jobs and Parquet writes; empty date or reconciliation raises |
| `batch-processing/validate_processed_transactions.py` | runners | schema, date, grain, null/range, directory validation | Spark reads/actions; nonzero exit on errors |
| `batch-processing/build_risk_features.py` | full runner | five builders, shared risk expressions, table writer | overwrites analytics Parquet |
| `batch-processing/validate_risk_features.py` | full runner | one validator per feature grain | Spark reads/actions; nonzero exit |
| `batch-processing/run_batch_pipeline.py` | cloud runner/Airflow | raw validation plus five fail-fast stages | rebuilds local canonical outputs |
| `cloud/sync_utils.py` | all sync scripts | command runner, local/GCS inventories, exact source/destination guards | calls gcloud; raises on listing/guard errors |
| `cloud/sync_processed_to_gcs.py` | cloud runner | preflight full processed layout; mirror; verify | deletes unmatched objects only under exact prefix |
| `cloud/sync_analytics_to_gcs.py` | cloud runner | validate all five, mirror each, verify exact names | five narrow GCS mirrors |
| `cloud/sync_processed_partition_to_gcs.py` | incremental/demo | parse date; preflight/sync one partition; compare unrelated inventory | one exact partition mirror |
| `warehouse/load_bigquery_tables.py` | cloud runner/Airflow | shared command; standard, high-risk, processed loaders | replaces six native tables |
| `warehouse/merge_processed_partition.py` | incremental/demo | stage load; stage validation; MERGE SQL; before/after metrics | replaces staging table, updates/inserts target |
| `warehouse/validate_bigquery_tables.py` | cloud runner | JSON query helper, `require`, six table validators | read-only BQ queries; raises contract errors |
| `pipeline/run_cloud_pipeline.py` | operator entry point | resolve dbt; `run_stage`; seven-stage main | local/GCS/BQ/dbt changes; fail-fast |
| `pipeline/run_incremental_pipeline.py` | daily CLI | date parser; six-stage main | one local/GCS/BQ date plus dbt |
| `pipeline/verify_late_arriving_data.py` | integration evidence | demo target, fixture, processing, MERGE assertions, evidence writer | isolated GCS/BQ demo resources and CSV evidence |
| `benchmarks/run_scale_tests.py` | local benchmark | command execution, metrics, per-scale run, evidence/report writers | ignored large data; tracked CSV/Markdown |
| `airflow/dags/risk_pipeline_dag.py` | Airflow scheduler | shell command builder and seven BashOperators | scheduled subprocess orchestration |
| `dbt_project.yml`, sources/schema YAML | dbt | materializations, lineage, generic tests | dbt compile/run/test metadata |
| staging SQL | dbt | source-aligned projections | BigQuery views |
| core SQL | dbt | incremental fact, customer/merchant/date dimensions | BQ merge/table builds |
| mart SQL | dbt | risk rankings, KPIs, trends, segment analysis | BQ tables |
| singular tests | dbt test | reconciliation, ranges, grains, risk logic | read-only invalid-row queries |
| Terraform files | Terraform CLI | providers, variables, resources, outputs | plan/apply manage GCS/BQ; never called by pipelines |

At 1 GB these functions are deliberately direct. At 1 TB, separate orchestration configuration from implementation and use managed compute. At 100 TB, replace filesystem/listing assumptions with catalog/table metadata and transactional publishing.
