# GCP Streaming Operations Runbook (v1.4)

This runbook applies only to the opt-in short GCP demo. For any uncertainty involving cost, first cancel the matching Dataflow jobs:

```bash
gcp_streaming/scripts/stop_dataflow_jobs.sh --cancel
make gcp-streaming-check-active
```

## Pub/Sub backlog grows

1. Check the subscription backlog and oldest unacked message age in Pub/Sub metrics.
2. Check Dataflow job state, system lag, worker errors, and the `valid_records`/`invalid_records` counters.
3. Do not raise the maximum worker count during the cost-controlled demo.
4. If the cause is not obvious within a few minutes, cancel the job.
5. Inspect quarantined payloads before restarting with the same subscription.

## Dataflow job fails

1. Record job ID/name, failure state, and the first actionable worker error.
2. Run the stop script to ensure no replacement or duplicate demo job is active.
3. Verify the worker service account, Dataflow API, Pub/Sub subscription access, dataset editor access, and GCS temp/staging access.
4. Check that all three BigQuery tables exist with the Terraform schemas.
5. Fix locally and rerun credential-free tests before another approved live attempt.

## BigQuery has no new rows

1. Confirm the producer reported successful Pub/Sub future results.
2. Check backlog: zero backlog can mean Dataflow consumed messages; growing backlog means it did not.
3. Inspect `streaming_transaction_events_quarantine` for schema or write errors.
4. Inspect `streaming_pipeline_observations` and the Dataflow `bigquery_write_failures` counter.
5. Verify project, dataset, table, region, and service-account dataset IAM.
6. Stop at the demo deadline even if diagnosis is incomplete.

## Invalid rate spikes

1. Compare `invalid_records` to valid plus invalid counters over the same interval.
2. Group quarantine rows by `error_reason`, `error_field`, and `run_id`.
3. Confirm producer interval flags and deterministic seed.
4. Do not weaken validation or dbt tests to make the alert disappear.
5. Stop publishing if unexpected schema drift is ongoing.

## dbt streaming tests fail

1. Stop Dataflow before running broad investigation queries.
2. Run only the streaming selection.
3. Check transaction ID uniqueness/non-nullness, event type, amount range, event date, timestamp ordering, quarantine reasons, and reconciliation failures.
4. Compare results by `run_id` so previous demo data does not obscure the current run.
5. Do not claim the GCP path as verified while tests fail.

## A cost alert fires

- $25: inventory all active jobs, topics/subscriptions, tables, logs, and Monitoring metrics.
- $50: cancel Dataflow unless the run is actively supervised and nearly complete.
- $75: cancel all matching demo jobs and perform full cleanup/review.
- $100: treat as an emergency; stop billable demo resources and inspect project-wide Billing reports.

Budget notifications can be delayed and do not cap spend. Use the active-resource checks, not the absence of an alert, as proof that the job stopped.

## Cleanup fails

1. Run `check_active_cost_resources.sh` to determine what still exists.
2. Cancel Dataflow first; do not delete its subscription while it is still running.
3. Verify exact project, region, dataset, and resource names.
4. Re-run individual, narrow commands rather than broad wildcards.
5. If Terraform manages a resource, reconcile with a plan before manually deleting it.
6. Never delete the whole `risk_analytics` dataset or processed-data bucket to clean this demo.

## Stop everything quickly

```bash
export GCP_PROJECT_ID="your-project-id"
export GCP_STREAMING_REGION="us-central1"
gcp_streaming/scripts/stop_dataflow_jobs.sh --cancel
gcp_streaming/scripts/check_active_cost_resources.sh
```

Then use the reviewed cleanup options in [29 — GCP cost controls and cleanup](29-gcp-cost-controls-and-cleanup.md). The stop script only targets `financial-risk-v14-demo-*`, so unrelated Dataflow jobs are not intentionally touched.
