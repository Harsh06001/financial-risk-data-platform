#!/bin/sh
set -eu

if [ "${1:-}" != "--acknowledge-cost-risk" ] && [ "${ACKNOWLEDGE_GCP_COST_RISK:-}" != "true" ]; then
  echo "ERROR: pass --acknowledge-cost-risk or set ACKNOWLEDGE_GCP_COST_RISK=true" >&2
  exit 2
fi

PROJECT_ID="${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
REGION="${GCP_STREAMING_REGION:-us-central1}"
EVENT_COUNT="${GCP_STREAMING_DEMO_EVENT_COUNT:-1000}"
MAX_MINUTES="${GCP_STREAMING_MAX_DEMO_MINUTES:-15}"
NUM_WORKERS="${GCP_STREAMING_NUM_WORKERS:-1}"
MAX_WORKERS="${GCP_STREAMING_MAX_WORKERS:-1}"
MACHINE_TYPE="${GCP_STREAMING_MACHINE_TYPE:-n1-standard-1}"
PYTHON_BIN="${PYTHON_BIN:-python3}"
JOB_NAME="financial-risk-v14-demo-$(date -u +%Y%m%d-%H%M%S)"
SERVICE_ACCOUNT="${GCP_STREAMING_SERVICE_ACCOUNT:-risk-streaming-dataflow@${PROJECT_ID}.iam.gserviceaccount.com}"
DATAFLOW_ROOT="gs://${PROJECT_ID}-streaming-dataflow-temp/v14-demo"

export ACKNOWLEDGE_GCP_COST_RISK=true
"$(dirname "$0")/preflight_cost_check.sh" --acknowledge-cost-risk

cleanup_on_exit() {
  "$(dirname "$0")/stop_dataflow_jobs.sh" --cancel --job-name "${JOB_NAME}" || true
}
trap cleanup_on_exit EXIT INT TERM

echo "WARNING: this command submits a billable, unbounded Dataflow job for at most ${MAX_MINUTES} minutes."
"${PYTHON_BIN}" -m gcp_streaming.beam_pipeline.streaming_to_bigquery \
  --runner DataflowRunner \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --subscription "projects/${PROJECT_ID}/subscriptions/transaction-events-dataflow-sub" \
  --dataset "${DBT_DATASET:-risk_analytics}" \
  --temp_location "${DATAFLOW_ROOT}/temp" \
  --staging_location "${DATAFLOW_ROOT}/staging" \
  --job_name "${JOB_NAME}" \
  --service_account_email "${SERVICE_ACCOUNT}" \
  --num_workers "${NUM_WORKERS}" \
  --max_num_workers "${MAX_WORKERS}" \
  --machine_type "${MACHINE_TYPE}" \
  --setup_file ./setup.py

"${PYTHON_BIN}" -m gcp_streaming.producer.publish_transaction_events \
  --project-id "${PROJECT_ID}" \
  --count "${EVENT_COUNT}" \
  --rate "${GCP_STREAMING_EVENT_RATE:-5}" \
  --acknowledge-cost-risk

echo "Demo events published. Waiting no longer than ${MAX_MINUTES} minutes before cancellation."
ELAPSED=0
MAX_SECONDS=$((MAX_MINUTES * 60))
while [ "${ELAPSED}" -lt "${MAX_SECONDS}" ]; do
  sleep 15
  ELAPSED=$((ELAPSED + 15))
  echo "demo_elapsed_seconds=${ELAPSED}"
done

cleanup_on_exit
trap - EXIT INT TERM
"$(dirname "$0")/check_active_cost_resources.sh"
