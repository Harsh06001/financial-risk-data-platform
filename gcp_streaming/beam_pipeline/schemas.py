"""Explicit BigQuery schemas used by the GCP streaming pipeline."""

VALID_EVENT_FIELDS = [
    {"name": "transaction_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "event_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "ingestion_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "processing_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "merchant_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "amount", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "currency", "type": "STRING", "mode": "REQUIRED"},
    {"name": "country", "type": "STRING", "mode": "REQUIRED"},
    {"name": "merchant_category", "type": "STRING", "mode": "REQUIRED"},
    {"name": "payment_method", "type": "STRING", "mode": "REQUIRED"},
    {"name": "device_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "event_type", "type": "STRING", "mode": "REQUIRED"},
    {"name": "event_date", "type": "DATE", "mode": "REQUIRED"},
    {"name": "event_hour", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "run_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "source_system", "type": "STRING", "mode": "REQUIRED"},
    {"name": "validation_status", "type": "STRING", "mode": "REQUIRED"},
]

QUARANTINE_FIELDS = [
    {"name": "raw_payload", "type": "STRING", "mode": "REQUIRED"},
    {"name": "error_reason", "type": "STRING", "mode": "REQUIRED"},
    {"name": "error_field", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ingestion_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
    {"name": "processing_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "run_id", "type": "STRING", "mode": "REQUIRED"},
]

OBSERVATION_FIELDS = [
    {"name": "observation_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    {"name": "run_id", "type": "STRING", "mode": "REQUIRED"},
    {"name": "metric_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "metric_value", "type": "FLOAT", "mode": "REQUIRED"},
    {"name": "status", "type": "STRING", "mode": "REQUIRED"},
    {"name": "severity", "type": "STRING", "mode": "REQUIRED"},
    {"name": "details", "type": "STRING", "mode": "NULLABLE"},
]

VALID_EVENT_SCHEMA = {"fields": VALID_EVENT_FIELDS}
QUARANTINE_SCHEMA = {"fields": QUARANTINE_FIELDS}
OBSERVATION_SCHEMA = {"fields": OBSERVATION_FIELDS}
