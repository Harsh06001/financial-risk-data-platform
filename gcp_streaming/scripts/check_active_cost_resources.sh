#!/bin/sh
set -eu

PROJECT_ID="${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
REGION="${GCP_STREAMING_REGION:-us-central1}"
DATASET="${DBT_DATASET:-risk_analytics}"

echo "=== Active Dataflow jobs in ${PROJECT_ID}/${REGION} ==="
gcloud dataflow jobs list \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --status=active \
  --format='table(id,name,state,createTime)'

echo "=== Demo Pub/Sub topics ==="
gcloud pubsub topics list \
  --project="${PROJECT_ID}" \
  --filter='name:(transaction-events OR transaction-events-dlq)' \
  --format='table(name)'

echo "=== Demo Pub/Sub subscriptions ==="
gcloud pubsub subscriptions list \
  --project="${PROJECT_ID}" \
  --filter='name:transaction-events-dataflow-sub' \
  --format='table(name,topic,messageRetentionDuration)'

echo "=== Streaming BigQuery tables ==="
bq ls --project_id="${PROJECT_ID}" "${PROJECT_ID}:${DATASET}" 2>/dev/null | \
  sed -n '/streaming_/p' || true

echo "Review Billing > Reports and Budgets & alerts in the Cloud Console."
echo "Cleanup command: make gcp-streaming-cleanup"
