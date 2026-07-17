# Resume Strategy

## Version 1.2 positioning

Describe v1.2 as a personal portfolio extension. Lead with the design boundary—verified canonical batch pipeline plus a controlled streaming simulation—and name only executed verification when supported. Prefer “implemented” or “built a local demo” over “operated,” “deployed,” or “productionized.” Never blend this GCP/Redpanda/dbt/Airflow work into professional experience unless it actually occurred there.

For v1.3, distinguish provisioned configuration from observed runtime. Prometheus/Grafana/Alertmanager files, dashboards, and integration automation are repository facts; operating a production monitoring platform is not.

For v1.4, “implemented and locally validated a cost-controlled Pub/Sub/Beam/Dataflow deployment mode” is claim-safe after the local checks pass. “Deployed,” “operated,” live row counts, runtime, throughput, and cost are not claim-safe until an approved live run is retained with cleanup and no-active-job evidence.

Maintain one factual experience inventory and tailor emphasis to each job description. ATS fit is job-specific; there is no defensible universal “85 ATS” version.

## 1. AWS / Spark Data Engineer

**Summary emphasis:** Two years of professional AWS and big-data engineering experience with Python, Spark/Scala on EMR, partitioned S3 data, Athena SQL, serverless workflows, production troubleshooting, and CI/CD. Mention the GCP portfolio as evidence of continued end-to-end platform development, not as Cognizant production experience.

**Skills order:** Python, SQL, Spark, Scala; AWS EMR, S3, Lambda, Athena, DynamoDB, API Gateway, CloudWatch; Parquet, partitioning, ETL, data quality; Git, Jenkins; then GCP/BigQuery/dbt/Airflow/Terraform under project or additional cloud skills.

**Cognizant bullet order:** Spark/EMR; Lambda workflow; Athena; millions-of-records workflow; production/CI delivery; collaboration.

**Project bullet order:** PySpark schema/partition processing; 248-to-31 file optimization; 10M local synthetic reconciliation; incremental MERGE architecture.

**Keywords to emphasize:** PySpark, Spark SQL, Scala, EMR, S3, distributed processing, Parquet, partitioning, ETL, Athena, data quality, batch processing.

**Remove or de-emphasize:** detailed dbt mart names, broad frontend collaboration, and GCP infrastructure details unless the job asks for multi-cloud exposure.

## 2. GCP / BigQuery Data Engineer

**Summary emphasis:** AWS/Spark professional experience plus a verified GCP portfolio demonstrating GCS, native BigQuery warehousing, partition-scoped incremental processing, staging-and-MERGE upserts, dbt dimensional models, Airflow orchestration, and Terraform.

**Skills order:** Python, SQL, PySpark; GCP, GCS, BigQuery; dbt, Airflow, Terraform; dimensional modeling, incremental processing, MERGE, Parquet, data quality; then AWS EMR/S3/Lambda/Athena.

**Cognizant bullet order:** millions-of-records workflow; Spark/EMR; Athena; Lambda; production delivery; collaboration. These show transferable engineering without relabeling AWS as GCP.

**Project bullet order:** BigQuery warehouse and validation; staging-and-MERGE late-data proof; dimensional dbt layer; guarded GCS synchronization; full/incremental orchestration; local scale evidence.

**Keywords to emphasize:** BigQuery, GCS, data warehousing, partitioning, MERGE, upsert, incremental processing, late-arriving data, dbt, Airflow, Terraform, dimensional modeling.

**Remove or de-emphasize:** long AWS service lists in the summary and low-level Spark-operation lists when the description centers on BigQuery architecture.

## 3. Analytics Engineer / dbt

**Summary emphasis:** SQL- and modeling-oriented data engineer with professional multi-source reporting experience and a verified dbt/BigQuery portfolio containing staging, dimensional core, marts, incremental facts, and contract/reconciliation tests.

**Skills order:** SQL, dbt, BigQuery; dimensional modeling, star schema, fact/dimension grains, data tests, reconciliation; Python, PySpark; GCS, Airflow, Terraform; AWS Athena/S3/EMR.

**Cognizant bullet order:** Athena reporting pipelines; millions-of-records data workflows; Spark transformations; Lambda validation; production delivery; collaboration.

**Project bullet order:** dbt staging/core/marts; incremental fact and relationship tests; BigQuery warehouse contracts; risk marts; full/incremental orchestration.

**Keywords to emphasize:** dbt, SQL, data modeling, dimensional modeling, fact tables, dimension tables, star schema, data quality, reconciliation, BigQuery, incremental models, analytics engineering.

**Remove or de-emphasize:** deep infrastructure implementation detail, local throughput figures unless requested, and generic software-development keywords that displace SQL/modeling evidence.

## Application workflow

1. Extract the must-have technologies and responsibilities from the specific job description.
2. Choose the matching master variant and retain only claims supported by professional facts or project evidence.
3. Put professional experience above the project; label the project clearly as a portfolio project.
4. Use two to four project bullets and four to six Cognizant bullets, prioritizing relevance over keyword volume.
5. Add recovered Cognizant metrics only after validating them with the questionnaire in [`cognizant-experience-audit.md`](cognizant-experience-audit.md).
6. Check keyword coverage against that specific description, then edit for readability and interview defensibility.
