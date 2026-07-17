#!/usr/bin/env sh
set -eu

BROKERS="${KAFKA_BOOTSTRAP_SERVERS:-redpanda:29092}"
TOPIC_PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-3}"
TOPIC_REPLICAS="${KAFKA_TOPIC_REPLICATION_FACTOR:-1}"

TOPICS="
transaction-events
transaction-events-dlq
streaming-risk-alerts
"

CLUSTER_INFO_LOG="/tmp/redpanda_cluster_info.log"

print_cluster_info_log() {
  if [ -f "${CLUSTER_INFO_LOG}" ]; then
    cat "${CLUSTER_INFO_LOG}"
  else
    echo "No Redpanda cluster-info log was written."
  fi
}

echo "Using Redpanda brokers: ${BROKERS}"
echo "Waiting for Redpanda to become available..."

READY=0
ATTEMPT=1

while [ "${ATTEMPT}" -le 60 ]; do
  if rpk cluster info -X brokers="${BROKERS}" > "${CLUSTER_INFO_LOG}" 2>&1; then
    READY=1
    break
  fi

  echo "Redpanda not ready yet. Attempt ${ATTEMPT}/60"
  print_cluster_info_log
  sleep 2

  ATTEMPT=$((ATTEMPT + 1))
done

if [ "${READY}" -ne 1 ]; then
  echo "ERROR: Redpanda did not become ready."
  echo "Last cluster-info output:"
  print_cluster_info_log
  exit 1
fi

echo "Redpanda is ready."
rpk cluster info -X brokers="${BROKERS}"

for topic in ${TOPICS}; do
  echo "Checking topic: ${topic}"

  if rpk topic describe "${topic}" -X brokers="${BROKERS}" > /dev/null 2>&1; then
    echo "Topic already exists: ${topic}"
  else
    echo "Creating topic: ${topic}"
    rpk topic create "${topic}" \
      --partitions "${TOPIC_PARTITIONS}" \
      --replicas "${TOPIC_REPLICAS}" \
      -X brokers="${BROKERS}"
  fi

  echo "Verifying topic: ${topic}"
  rpk topic describe "${topic}" -X brokers="${BROKERS}"
done

echo "Topic initialization completed successfully."