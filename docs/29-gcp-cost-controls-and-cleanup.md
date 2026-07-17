# GCP Cost Controls and Cleanup (v1.4)

## Read this before a live demo

The account may have about $300 in credits. Credits are not a spending cap. This implementation is designed for a short, low-volume demonstration with a usage target of roughly $30–$50 or less, but actual cost depends on region, runtime, worker availability, service pricing, logging, monitoring, storage, and prior project usage. No exact cost is guaranteed.

Dataflow is the main risk because a Pub/Sub streaming job is unbounded and continues using workers until drained or cancelled. Pub/Sub retention, BigQuery storage/queries/streaming writes, GCS staging, Cloud Logging, Cloud Monitoring custom metrics, and optional alert policies can also cost money.

## Hard defaults

| Control | Default | Enforced by |
|---|---:|---|
| GCP resources enabled | false | Terraform flags |
| Events | 1,000 | Terraform, producer, preflight |
| Maximum demo window | 15 minutes | preflight and launcher |
| Initial workers | 1 | Terraform, preflight, Beam |
| Maximum workers | 1 | Terraform, preflight, Beam |
| Machine type | `n1-standard-1` | environment/Terraform |
| Streaming partition retention | 7 days | Terraform (1–30 day guard) |
| Monitoring policies | false | Terraform |
| Budget resource | false | Terraform |
| Cleanup mutation | false | cleanup script |

The producer refuses more than 10,000 events without an explicit override. The guarded preflight rejects more than 10,000 events, more than 15 minutes, more than one initial worker, more than two maximum workers, project mismatches, disabled billing, missing identity, and missing cost acknowledgement.

## Budget alerts

Before deployment, use Cloud Console → Billing → Budgets & alerts. Create a project-scoped monthly $100 budget with actual-spend thresholds:

- $25: warning and re-check active resources;
- $50: serious warning, stop the demo unless actively observing it;
- $75: stop and inspect all billable resources;
- $100: emergency response and project-wide cost review.

A budget sends notifications; it does not automatically cap or stop spend. Delivery and billing data can be delayed. Configure appropriate email/Monitoring notification recipients and verify them.

Optional Terraform models the same $100 project-scoped budget with 25%, 50%, 75%, and 100% thresholds. It is disabled by default and fails when enabled without an explicitly supplied billing account ID:

```bash
terraform -chdir=infrastructure/terraform plan \
  -var=enable_budget_alerts=true \
  -var='billing_account_id=REVIEWED-BILLING-ACCOUNT-ID'
```

Never put the ID in a committed `.tfvars` file. Review billing-account scope and permissions carefully before applying.

## Preflight

```bash
export GCP_PROJECT_ID="your-project-id"
export GCP_STREAMING_REGION="us-central1"
export GCP_STREAMING_DEMO_EVENT_COUNT="1000"
export GCP_STREAMING_MAX_DEMO_MINUTES="15"
export GCP_STREAMING_NUM_WORKERS="1"
export GCP_STREAMING_MAX_WORKERS="1"
export ACKNOWLEDGE_GCP_COST_RISK=true
make gcp-streaming-preflight
```

The check prints the project, billing-enabled state, active account, region, events, duration, workers, machine type, and acknowledgement. It creates nothing.

## Stop immediately

Read-only inventory:

```bash
make gcp-streaming-check-active
gcp_streaming/scripts/stop_dataflow_jobs.sh
```

Cancel only jobs whose names begin `financial-risk-v14-demo-`:

```bash
gcp_streaming/scripts/stop_dataflow_jobs.sh --cancel
```

Drain instead when processing the remaining tiny backlog matters more than an immediate stop:

```bash
gcp_streaming/scripts/stop_dataflow_jobs.sh --drain
```

Cancellation is the cost-first emergency action. Draining can continue consuming resources until completion.

Confirm no active jobs:

```bash
gcloud dataflow jobs list \
  --project="${GCP_PROJECT_ID}" \
  --region="${GCP_STREAMING_REGION:-us-central1}" \
  --status=active
```

## Cleanup is explicit and scoped

The default command is read-only:

```bash
make gcp-streaming-cleanup
```

After reviewing its output, delete only the guarded Dataflow temp/staging prefix:

```bash
GCP_STREAMING_CLEANUP_ARGS='--execute --delete-temp' make gcp-streaming-cleanup
```

Optional demo data deletion must be intentional:

```bash
gcp_streaming/scripts/cleanup_streaming_demo.sh \
  --execute \
  --delete-bigquery-tables
```

Optional demo messaging deletion must also be intentional. Do not use it when Terraform should retain/manage the resources:

```bash
gcp_streaming/scripts/cleanup_streaming_demo.sh \
  --execute \
  --delete-topics
```

The script only recognizes the exact subscription, exact topics, exact three tables, and `gs://${GCP_PROJECT_ID}-streaming-dataflow-temp/v14-demo/{temp,staging}`. It never deletes the dataset, canonical batch tables, canonical processed bucket, or whole temp bucket. Terraform applies a one-day object lifecycle to the dedicated temp bucket as a second cleanup layer.

If Cloud Monitoring policies were created solely for the demo, remove them through a reviewed Terraform plan. Do not manually delete shared notification channels. If Terraform created the messaging/table resources, prefer a reviewed targeted lifecycle plan rather than mixing manual and managed deletion.

## Post-run checklist

1. Cancel or drain the matching Dataflow job.
2. Repeat the active-job listing until no demo job is active.
3. Inspect the exact Pub/Sub subscription backlog and decide whether to retain or delete it.
4. List streaming BigQuery tables and preserve only evidence intentionally needed.
5. Delete the guarded GCS temp/staging prefixes if appropriate.
6. Check Monitoring policies and custom-metric usage.
7. Open Cloud Console → Billing → Reports and filter by project and service.
8. Open Budgets & alerts and confirm notifications are configured.
9. Run `make gcp-streaming-check-active` again.

Resources intentionally left after a normal demo are the Terraform-managed topics, subscription, three small tables, service account/IAM, and optional policies. Stored rows, retained messages, logs, metrics, and staging objects may continue to incur small charges even after Dataflow stops, so inspect and clean them deliberately.
