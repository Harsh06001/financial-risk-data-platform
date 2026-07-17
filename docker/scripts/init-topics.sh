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

echo "Using Redpanda brokers: ${BROKERS}"
echo "Waiting for Redpanda to become available..."

READY=0

for i in $(seq 1 60); do
  if rpk cluster info -X brokers="${BROKERS}" >/tmp/redpanda_cluster_info.log 2>&1; then
    READY=1
    break
  fi

  echo "Redpanda not ready yet. Attempt ${i}/60"
  cat /tmp/redpanda_cluster_info.log || true
  sleep 2
done

if [ "${READY}" -ne 1 ]; then
  echo "ERROR: Redpanda did not become ready."
  echo "Last cluster-info output:"
  cat /tmp/redpanda_cluster_info.log || true
  exit 1
fi

echo "Redpanda is ready."
rpk cluster info -X brokers="${BROKERS}"

for topic in ${TOPICS}; do
  echo "Checking topic: ${topic}"

  if rpk topic describe "${topic}" -X brokers="${BROKERS}" >/dev/null 2>&1; then
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