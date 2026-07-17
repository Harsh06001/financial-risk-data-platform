#!/bin/sh
set -eu

PROJECT_ID="${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
REGION="${GCP_STREAMING_REGION:-us-central1}"
MODE="list"
JOB_NAME=""

while [ "$#" -gt 0 ]; do
  case "$1" in
    --cancel|--drain)
      MODE="${1#--}"
      ;;
    --job-name)
      shift
      JOB_NAME="${1:?--job-name requires a value}"
      ;;
    *)
      echo "ERROR: unknown argument: $1" >&2
      exit 2
      ;;
  esac
  shift
done

JOBS="$(gcloud dataflow jobs list \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --status=active \
  --filter='name~^financial-risk-v14-demo-' \
  --format='value(id,name)')"

if [ -z "${JOBS}" ]; then
  echo "No active financial-risk-v14-demo-* Dataflow jobs found."
else
  printf '%s\n' "${JOBS}"
fi

if [ "${MODE}" = "list" ]; then
  echo "Read-only mode. Re-run with --cancel or --drain to stop only matching demo jobs."
else
  printf '%s\n' "${JOBS}" | while read -r JOB_ID FOUND_NAME; do
    [ -n "${JOB_ID}" ] || continue
    if [ -n "${JOB_NAME}" ] && [ "${FOUND_NAME}" != "${JOB_NAME}" ]; then
      continue
    fi
    echo "Requesting ${MODE} for ${FOUND_NAME} (${JOB_ID})"
    gcloud dataflow jobs "${MODE}" "${JOB_ID}" \
      --project="${PROJECT_ID}" \
      --region="${REGION}"
  done

  ATTEMPT=0
  while [ "${ATTEMPT}" -lt 18 ]; do
    REMAINING="$(gcloud dataflow jobs list \
      --project="${PROJECT_ID}" \
      --region="${REGION}" \
      --status=active \
      --filter='name~^financial-risk-v14-demo-' \
      --format='value(name)')"
    if [ -n "${JOB_NAME}" ]; then
      REMAINING="$(printf '%s\n' "${REMAINING}" | sed -n "/^${JOB_NAME}$/p")"
    fi
    [ -n "${REMAINING}" ] || break
    ATTEMPT=$((ATTEMPT + 1))
    echo "Waiting for Dataflow stop to settle (${ATTEMPT}/18): ${REMAINING}"
    sleep 10
  done
  if [ -n "${REMAINING:-}" ]; then
    echo "ERROR: matching demo job is still active after stop wait" >&2
    exit 1
  fi
fi

echo "=== Active demo jobs after stop request ==="
gcloud dataflow jobs list \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --status=active \
  --filter='name~^financial-risk-v14-demo-' \
  --format='table(id,name,state)'

echo "=== Demo subscription ==="
gcloud pubsub subscriptions list \
  --project="${PROJECT_ID}" \
  --filter='name:transaction-events-dataflow-sub' \
  --format='table(name,topic)'

echo "=== Streaming tables (not deleted) ==="
bq ls --project_id="${PROJECT_ID}" "${PROJECT_ID}:${DBT_DATASET:-risk_analytics}" 2>/dev/null | \
  sed -n '/streaming_/p' || true
echo "Use gcp_streaming/scripts/cleanup_streaming_demo.sh for explicit cleanup options."
