# Partitioning and Small Files

Processed transactions and high-risk transactions use `event_date=YYYY-MM-DD` directories. This enables partition pruning and date-scoped synchronization/loading. Partitioning is valuable because event date is commonly filtered and has moderate cardinality.

Too many files increase listing, metadata, scheduling, and open costs. Too few files can reduce parallelism. The verified 100,350-row optimization deliberately targets one file per date. The local 1M, 5M, and 10M synthetic tests also produced 31 files, but that is evidence for this machine and workload, not a universal production target.

Dynamic partition overwrite is required for incremental processing: overwrite only partitions present in the write DataFrame. A normal full overwrite could remove all unrelated dates.

At 1 GB, one file per date is acceptable. At 1 TB, target useful file sizes and compact small arrivals. At 100 TB, use table formats or scheduled compaction, partition evolution, clustering, and metadata services; date partitioning alone may be insufficient.
