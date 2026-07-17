# Controlled Streaming Demo

`transaction-events` is a one-partition Redpanda topic used for a bounded portfolio demonstration. The producer is deterministic for a fixed seed and can inject controlled invalid, duplicate, and late events. It never writes to the canonical batch source. Invalid raw JSON may also be published to `transaction-events-dlq`, and clean events at or above the configured amount may be published to `streaming-risk-alerts`.

The Spark consumer parses JSON with an explicit schema and uses `foreachBatch`. Each micro-batch writes:

- all parsed Kafka records to `data/streaming/bronze/transaction_events/`;
- valid, micro-batch-deduplicated rows to `data/streaming/silver/transaction_events/`;
- invalid rows to `data/streaming/quarantine/transaction_events/`;
- reconciliation metadata to `data/streaming/metrics/transaction_events/`;
- broker offsets and state to `data/streaming/checkpoints/transaction_events/`.

`availableNow` is the normal demo trigger, so the consumer drains currently available records and stops. Checkpoint reuse prevents reprocessing acknowledged Kafka offsets. Duplicate handling is intentionally scoped to a micro-batch; the optional BigQuery loader deduplicates the staged snapshot by transaction ID before MERGE.

Use `make stream-restart-snapshot`, rerun `make stream-process` without producing more records, and then run `make stream-restart-verify` to verify that the preserved checkpoint did not change the accounted batch fingerprint.

Dry-run producer validation requires no broker:

```bash
.venv/bin/python -m streaming.producer.produce_transaction_events \
  --count 100 --seed 202612 \
  --dry-run-output data/streaming/dry-run/events.jsonl
```

See `docs/28-v1-3-streaming-runtime-and-monitoring.md` for the complete local deployment.
