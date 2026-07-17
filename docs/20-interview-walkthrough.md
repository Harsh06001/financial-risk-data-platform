# Interview Walkthrough

For v1.2, explain the boundary first: the verified batch warehouse remains canonical, while streaming is a bounded Kafka-compatible simulation. Walk through explicit schema enforcement, quarantine, per-batch deduplication and reconciliation, checkpoints, staged BigQuery `MERGE`, opt-in dbt models, local-first observations, and credential-free CI. Be ready to distinguish implemented code from services not executable in the current environment and from production improvements.

Use this sequence for a 10–15 minute explanation:

1. State the problem: trustworthy batch risk analytics from transaction events.
2. Draw full and incremental paths and name their different overwrite semantics.
3. Explain the explicit Spark schema, lazy transformations, dedup shuffle, and date repartition optimization.
4. Show validation at raw, processed, feature, GCS, BigQuery, and dbt boundaries.
5. Explain native versus external BigQuery tables.
6. Define the fact and dimension grains and why natural keys/Type 1 are appropriate.
7. Walk through staging and MERGE for same-date reruns, late inserts, and corrections.
8. Give only verified numbers and name their evidence files.
9. Close with limitations and what would change at 1 TB/100 TB.

Questions to practice:

- Why does `dropDuplicates` shuffle?
- Why did baseline writing create 248 files?
- Why is dynamic partition overwrite safer for one date?
- How do you prove GCS reruns do not accumulate stale Parquet objects?
- Why does MERGE use `transaction_id`, and what happens if a correction changes date?
- Why is the fact incremental but the dimensions are tables?
- What did the failed timezone scale attempt reveal?
- Which claims are local synthetic versus canonical benchmark evidence?

## Version 1.4 discussion

Describe v1.4 as an implemented, cost-controlled deployment mode—not as production experience or a live-verified deployment. Explain why the local Redpanda/Spark mode remains useful for credential-free runtime testing while Pub/Sub/Dataflow demonstrates a managed GCP architecture.

Be ready to explain:

- why all Terraform creation flags default to false;
- why a Pub/Sub streaming job cannot honestly be called bounded;
- how explicit validation, quarantine, BigQuery insert-error routing, observations, and Beam counters work;
- why the demo fixes one initial/maximum worker and stops after 10–15 minutes;
- how the producer refuses large/live sends without explicit overrides and acknowledgement;
- why Cloud Billing budgets are notifications rather than hard caps;
- how stop scripts restrict matching to `financial-risk-v14-demo-*`; and
- the evidence boundary: local compilation/tests/parse/validation versus an unexecuted live GCP deployment.
