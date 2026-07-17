#!/bin/sh
set -eu

PYTHON_BIN="${PYTHON_BIN:-python3}"
PROJECT_ID="${GCP_PROJECT_ID:-}"

"${PYTHON_BIN}" -m gcp_streaming.cost_controls "$@"

for command_name in gcloud bq; do
  if ! command -v "${command_name}" >/dev/null 2>&1; then
    echo "ERROR: required command not found: ${command_name}" >&2
    exit 2
  fi
done

ACTIVE_PROJECT="$(gcloud config get-value project 2>/dev/null || true)"
if [ -z "${ACTIVE_PROJECT}" ] || [ "${ACTIVE_PROJECT}" = "(unset)" ]; then
  echo "ERROR: gcloud has no active project" >&2
  exit 2
fi
if [ "${ACTIVE_PROJECT}" != "${PROJECT_ID}" ]; then
  echo "ERROR: gcloud project ${ACTIVE_PROJECT} does not match GCP_PROJECT_ID ${PROJECT_ID}" >&2
  exit 2
fi

ACTIVE_ACCOUNT="$(gcloud auth list --filter=status:ACTIVE --format='value(account)' 2>/dev/null)"
if [ -z "${ACTIVE_ACCOUNT}" ]; then
  echo "ERROR: no active gcloud account" >&2
  exit 2
fi

BILLING_ENABLED="$(gcloud billing projects describe "${PROJECT_ID}" --format='value(billingEnabled)' 2>/dev/null || true)"
if [ "${BILLING_ENABLED}" != "True" ]; then
  echo "ERROR: active project does not report billingEnabled=True" >&2
  exit 2
fi

echo "active_gcloud_account=${ACTIVE_ACCOUNT}"
echo "active_gcloud_project=${ACTIVE_PROJECT}"
echo "billing_enabled=${BILLING_ENABLED}"
echo "LIVE PREFLIGHT PASSED: this check did not create resources."
