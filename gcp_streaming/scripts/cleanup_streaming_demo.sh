#!/bin/sh
set -eu

PROJECT_ID="${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
REGION="${GCP_STREAMING_REGION:-us-central1}"
DATASET="${DBT_DATASET:-risk_analytics}"
EXECUTE=false
DELETE_TOPICS=false
DELETE_TABLES=false
DELETE_TEMP=false

while [ "$#" -gt 0 ]; do
  case "$1" in
    --execute) EXECUTE=true ;;
    --delete-topics) DELETE_TOPICS=true ;;
    --delete-bigquery-tables) DELETE_TABLES=true ;;
    --delete-temp) DELETE_TEMP=true ;;
    *) echo "ERROR: unknown argument: $1" >&2; exit 2 ;;
  esac
  shift
done

echo "Cleanup scope: project=${PROJECT_ID} region=${REGION} dataset=${DATASET}"
echo "By default this script is read-only. Destructive options require --execute."

if [ "${EXECUTE}" = true ]; then
  "$(dirname "$0")/stop_dataflow_jobs.sh" --cancel

  if [ "${DELETE_TOPICS}" = true ]; then
    if gcloud pubsub subscriptions describe transaction-events-dataflow-sub \
      --project="${PROJECT_ID}" >/dev/null 2>&1; then
      gcloud pubsub subscriptions delete transaction-events-dataflow-sub \
        --project="${PROJECT_ID}" --quiet
    fi
    for TOPIC in transaction-events transaction-events-dlq; do
      if gcloud pubsub topics describe "${TOPIC}" \
        --project="${PROJECT_ID}" >/dev/null 2>&1; then
        gcloud pubsub topics delete "${TOPIC}" \
          --project="${PROJECT_ID}" --quiet
      fi
    done
  fi

  if [ "${DELETE_TABLES}" = true ]; then
    for TABLE in streaming_transaction_events streaming_transaction_events_quarantine streaming_pipeline_observations; do
      if bq show "${PROJECT_ID}:${DATASET}.${TABLE}" >/dev/null 2>&1; then
        bq rm -f -t "${PROJECT_ID}:${DATASET}.${TABLE}"
      fi
    done
  fi

  if [ "${DELETE_TEMP}" = true ]; then
    TEMP_ROOT="gs://${PROJECT_ID}-streaming-dataflow-temp/v14-demo"
    case "${TEMP_ROOT}" in
      gs://*-streaming-dataflow-temp/v14-demo)
        for PREFIX in temp staging; do
          if gcloud storage ls "${TEMP_ROOT}/${PREFIX}/**" >/dev/null 2>&1; then
            gcloud storage rm --recursive "${TEMP_ROOT}/${PREFIX}/**"
          fi
        done
        ;;
      *) echo "ERROR: refusing unsafe GCS cleanup prefix: ${TEMP_ROOT}" >&2; exit 2 ;;
    esac
  fi
else
  echo "Would cancel only active financial-risk-v14-demo-* Dataflow jobs."
  [ "${DELETE_TOPICS}" = false ] || echo "Would delete the exact demo subscription and two exact demo topics."
  [ "${DELETE_TABLES}" = false ] || echo "Would delete exactly three streaming demo tables."
  [ "${DELETE_TEMP}" = false ] || echo "Would delete only gs://${PROJECT_ID}-streaming-dataflow-temp/v14-demo/{temp,staging}."
fi

"$(dirname "$0")/check_active_cost_resources.sh"
echo "If demo-only alert policies were applied, remove them with a reviewed Terraform plan."
