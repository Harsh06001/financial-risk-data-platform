# BigQuery MERGE and Late Data

`warehouse/merge_processed_partition.py` loads one GCS partition into a replaceable staging table, validates nonempty/unique IDs and exact date membership, then runs:

```mermaid
flowchart LR
    A[One GCS date] --> B[Replace staging table]
    B --> C{transaction_id match?}
    C -->|yes| D[Update all mutable processed columns]
    C -->|no| E[Insert transaction]
    D --> F[Validate target uniqueness]
    E --> F
```

Matched records are updated because a late correction can change amount or descriptive fields. Unmatched IDs are inserted. The target remains partitioned by `event_date`; updating a corrected date can move a row between partitions if justified by source data.

The isolated demo uses a separate GCS prefix and BigQuery target. Verified evidence in `benchmarks/results/incremental_merge_evidence.csv` shows:

- baseline: 2,640 rows;
- first and unchanged rerun: 2,640 rows;
- late/corrected merge: 2,641 rows;
- one late insert, one corrected update, zero duplicates.

The fixture selects an ID known to exist in the canonical target partition, pins its corrected timestamp safely inside the selected UTC date, and never edits canonical raw CSV files.
