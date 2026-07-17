#!/bin/sh
set -eu

BROKERS="${KAFKA_BOOTSTRAP_SERVERS:-redpanda:29092}"

ensure_topic() {
    topic="$1"
    if rpk topic describe "$topic" --brokers "$BROKERS" >/dev/null 2>&1; then
        echo "TOPIC EXISTS: $topic"
    else
        rpk topic create "$topic" --brokers "$BROKERS" --partitions 1 --replicas 1
        rpk topic describe "$topic" --brokers "$BROKERS" >/dev/null
        echo "TOPIC CREATED: $topic"
    fi
}

ensure_topic "${KAFKA_TOPIC:-transaction-events}"
ensure_topic "${KAFKA_DLQ_TOPIC:-transaction-events-dlq}"
ensure_topic "${KAFKA_RISK_ALERT_TOPIC:-streaming-risk-alerts}"

rpk cluster config set enable_consumer_group_metrics \
    '["group", "partition", "consumer_lag"]' \
    --no-confirm \
    -X "brokers=$BROKERS"
rpk cluster config set consumer_group_lag_collection_interval_sec 15 \
    --no-confirm \
    -X "brokers=$BROKERS"

echo "TOPIC INITIALIZATION COMPLETE"
