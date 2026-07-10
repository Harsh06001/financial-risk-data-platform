# Cognizant Experience Audit

Role: **Software Engineer (AWS & Big Data), July 2022–August 2024**

This audit uses only the professional facts supplied by the candidate. GCP, BigQuery, dbt, Airflow, and Terraform are personal-project technologies and must not be transferred into Cognizant bullets.

## Bullet-by-bullet audit

| # | Assessment | Improvement focus |
|---:|---|---|
| 1 | Strong Data Engineer relevance and credible scale through “millions of company records,” but the ownership and resulting use are broad. | Name the workflow stages and defensible downstream purpose; recover cadence, volume, consumers, or SLA. |
| 2 | Strong AWS serverless depth and good end-to-end lifecycle. It is long and lacks scale/outcome evidence. | Keep the state/failure-handling detail, then add requests, workflows, latency, or manual effort only if verified. |
| 3 | Strong Big Data relevance and ATS coverage. The list of Spark operations risks reading like a skills inventory. | Tie transformations to multi-source datasets and a verified performance, volume, or reliability outcome. |
| 4 | Strong SQL and lake-querying relevance. “Created” shows ownership, but the reporting consumer and query behavior need evidence. | Recover dataset volume, query runtime, scan/cost change, refresh cadence, or consumer count. |
| 5 | Credible production ownership and delivery lifecycle. It combines incident response and release engineering but has no resolution/deployment measure. | Recover incident frequency, mean recovery time, deployment frequency/time, or escaped-defect change. |
| 6 | Useful collaboration evidence and a verified seven-person team size. It is the least technically distinctive bullet. | Keep for behavioral coverage or merge into delivery ownership when space is tight; recover number of partner teams/releases. |

## Conservative versions: verified facts only

1. Built and supported data workflows processing millions of company records across business and financial sources into customer-facing reports and downstream data products.

2. Developed Python AWS Lambda workflows integrating API Gateway, S3, DynamoDB, and downstream APIs to ingest JSON/CSV, validate and transform records, persist workflow state, generate outputs, and handle failures.

3. Implemented distributed Spark and Scala transformations on AWS EMR using multi-source joins, filters, aggregations, window functions, UDFs, and partitioned S3 reads and writes.

4. Created Athena external tables over 3–4 S3-backed sources and developed SQL pipelines with CTEs, joins, aggregations, and partition-aware queries for integrated reporting datasets.

5. Diagnosed production failures with CloudWatch Logs and delivered fixes through Git pull requests, code review, Jenkins CI/CD, versioned releases, and QA validation.

6. Collaborated within a seven-member big-data engineering team and with QA and frontend teams to validate outputs, resolve integration issues, and coordinate production-ready releases.

## Stronger templates: use only after recovering evidence

1. Engineered [VERIFY: number of workflows] workflows processing [VERIFY: records per day or batch] company records across [VERIFY: source count/types], delivering datasets to [VERIFY: downstream consumer count/use] within [VERIFY: SLA or refresh cadence].

2. Developed Python Lambda workflows across API Gateway, S3, DynamoDB, and downstream APIs to process [VERIFY: requests/files/records per day], reducing [VERIFY: manual effort or latency] while maintaining [VERIFY: failure/retry or completion measure].

3. Implemented Spark/Scala transformations on EMR for [VERIFY: input size/record count], using partitioned S3 I/O, joins, windows, and aggregations to improve runtime from [VERIFY: before] to [VERIFY: after] or meet [VERIFY: batch SLA].

4. Modeled Athena external tables over 3–4 S3 sources and authored partition-aware SQL reporting pipelines, reducing [VERIFY: query runtime or scanned bytes/cost] and serving [VERIFY: reports/consumers/refresh cadence].

5. Diagnosed [VERIFY: incident count/frequency] production failures through CloudWatch and shipped reviewed fixes through Jenkins and QA, improving [VERIFY: recovery time, recurrence, deployment time, or release reliability].

6. Coordinated delivery across a seven-member big-data team, QA, and frontend for [VERIFY: release/workflow count], resolving [VERIFY: integration issue type] and meeting [VERIFY: release or validation outcome].

## Metric recovery questionnaire

Answer from records, performance reviews, tickets, dashboards, or defensible memory. Use ranges only when you can explain how they were derived; omit any metric you cannot defend in an interview.

### Workload and scale

- How many records arrived per day, batch, or month? What was the peak?
- What were typical and peak S3 input/output sizes?
- How many Lambda requests, files, jobs, or distinct workflows ran per day?
- How many source systems and downstream consumers were involved?
- How often were reports or datasets refreshed?

### Spark and SQL performance

- What was the Spark runtime before and after your change?
- What cluster size or EMR configuration can you verify?
- What was the Athena query runtime before and after optimization?
- Did partition pruning reduce scanned bytes or query cost? By how much, if recorded?
- What latency or batch-completion SLA existed, and how often was it met?

### Automation and business workflow

- What manual process did the Lambda or data workflow replace?
- How many person-hours or handoffs were removed per run/week/month?
- How long did report generation take before and after?
- How many reports, data products, teams, or customers consumed the outputs?

### Reliability and delivery

- How many incidents did you investigate per month or quarter?
- What was typical time to detect and time to recover?
- Did a fix reduce recurring failures? What evidence supports it?
- How frequently did the team deploy, and how long did deployment take?
- Did Jenkins automation change release time or failure rate?
- How many releases or production fixes did you personally deliver?

### Ownership

- Which components did you design versus maintain?
- Which technical decisions did you own or propose?
- Did you review others’ code, mentor teammates, or lead an incident?
- What scope can a manager or teammate corroborate?

## Honest positioning

- **Data Engineer I:** strongest fit; lead with hands-on Python, Spark/Scala, S3, Athena, data quality, and production support.
- **AWS Data Engineer:** strong fit when the description emphasizes Lambda, EMR, S3, DynamoDB, Athena, and CloudWatch.
- **Big Data Engineer:** strong fit when the role values Spark/Scala, distributed transformations, partitioned storage, and SQL.
- **Software Engineer, Data Platform:** strong fit when the role combines APIs/serverless workflows, data processing, observability, and delivery ownership.
- **Data Engineer II:** reasonable stretch where the scope matches; strengthen the case with verified design ownership, measurable performance/reliability outcomes, and end-to-end responsibility—not inflated years.

The two years of Cognizant experience remain two years of professional experience. The GCP portfolio project demonstrates current breadth but does not convert that tenure into four years.
