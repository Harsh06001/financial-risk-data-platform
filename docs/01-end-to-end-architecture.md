# End-to-End Architecture

Version 1.2 adds a deliberately separate simulation path: deterministic events flow through Redpanda and bounded Spark Structured Streaming into local bronze/silver/quarantine artifacts, with an optional host-side BigQuery `MERGE` and opt-in dbt models. Lightweight observations feed console/file/optional Slack alerts. This extension does not change the canonical v1.1 batch path or its evidence; see [the streaming design](23-streaming-pipeline.md).

```mermaid
flowchart TD
    A[Raw CSV] --> B{Run mode}
    B -->|Full| C[Full PySpark ETL]
    B -->|One date| D[Filtered PySpark ETL]
    C --> E[Processed Parquet by event_date]
    D --> E
    E --> F[GCS processed layer]
    C --> G[Five risk feature tables]
    G --> H[GCS analytics layer]
    F --> I[BigQuery processed_transactions]
    D --> J[BigQuery staging table]
    J -->|MERGE transaction_id| I
    H --> K[Native feature tables]
    H -. direct Parquet query .-> L[daily_transaction_summary_external]
    I --> M[dbt staging]
    K --> M
    M --> N[fct_transactions and dimensions]
    N --> O[Analytics marts]
```

The design separates storage concerns. Local and GCS Parquet are lake-style physical layers. BigQuery native tables are the warehouse source for dbt. The external table remains a demonstration of query-in-place behavior and is not a dbt production source.

Full batch sequence:

```mermaid
sequenceDiagram
    participant R as Runner
    participant S as Spark
    participant G as GCS
    participant B as BigQuery
    participant D as dbt
    R->>S: validate, transform, feature, validate
    R->>G: exact-prefix mirror sync
    R->>B: replace native tables and validate
    R->>D: run and test
```

All subprocess stages use nonzero exit status for linear failure propagation.

## Version 1.4 opt-in GCP streaming branch

The existing batch and local Redpanda/Spark paths remain intact. Version 1.4 adds a separately enabled deployment definition:

```mermaid
flowchart LR
    A[Synthetic producer] --> B[Pub/Sub topic]
    B --> C[Dataflow subscription]
    C --> D[Beam validation and routing]
    D -->|valid| E[BigQuery streaming events]
    D -->|invalid/write failure| F[BigQuery quarantine]
    D --> G[BigQuery observations]
    E --> H[Opt-in dbt streaming marts]
    D --> I[Dataflow user counters]
    B --> J[Pub/Sub backlog metric]
    I --> K[Optional Cloud Monitoring alerts]
    J --> K
```

Terraform defaults every v1.4 creation flag to false. The short demo uses one worker and 1,000 events, requires cost acknowledgement, and cancels only jobs named `financial-risk-v14-demo-*`. This branch is implemented and locally parsed/tested but is not represented as a live-verified or production deployment.
