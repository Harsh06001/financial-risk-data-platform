# End-to-End Architecture

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
