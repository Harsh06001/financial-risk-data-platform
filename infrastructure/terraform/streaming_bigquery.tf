resource "google_bigquery_table" "streaming_transaction_events" {
  count = var.enable_gcp_streaming ? 1 : 0

  project             = var.project_id
  dataset_id          = google_bigquery_dataset.risk_analytics.dataset_id
  table_id            = "streaming_transaction_events"
  deletion_protection = false

  time_partitioning {
    type          = "DAY"
    field         = "event_date"
    expiration_ms = var.streaming_partition_retention_days * 24 * 60 * 60 * 1000
  }

  clustering = ["customer_id", "merchant_id", "event_type", "validation_status"]
  labels     = local.streaming_labels
  schema = jsonencode([
    { name = "transaction_id", type = "STRING", mode = "REQUIRED" },
    { name = "event_timestamp", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "ingestion_timestamp", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "processing_timestamp", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "customer_id", type = "STRING", mode = "REQUIRED" },
    { name = "merchant_id", type = "STRING", mode = "REQUIRED" },
    { name = "amount", type = "FLOAT", mode = "REQUIRED" },
    { name = "currency", type = "STRING", mode = "REQUIRED" },
    { name = "country", type = "STRING", mode = "REQUIRED" },
    { name = "merchant_category", type = "STRING", mode = "REQUIRED" },
    { name = "payment_method", type = "STRING", mode = "REQUIRED" },
    { name = "device_id", type = "STRING", mode = "REQUIRED" },
    { name = "event_type", type = "STRING", mode = "REQUIRED" },
    { name = "event_date", type = "DATE", mode = "REQUIRED" },
    { name = "event_hour", type = "INTEGER", mode = "REQUIRED" },
    { name = "run_id", type = "STRING", mode = "REQUIRED" },
    { name = "source_system", type = "STRING", mode = "REQUIRED" },
    { name = "validation_status", type = "STRING", mode = "REQUIRED" },
  ])
}

resource "google_bigquery_table" "streaming_transaction_events_quarantine" {
  count = var.enable_gcp_streaming ? 1 : 0

  project             = var.project_id
  dataset_id          = google_bigquery_dataset.risk_analytics.dataset_id
  table_id            = "streaming_transaction_events_quarantine"
  deletion_protection = false

  time_partitioning {
    type          = "DAY"
    field         = "processing_timestamp"
    expiration_ms = var.streaming_partition_retention_days * 24 * 60 * 60 * 1000
  }

  clustering = ["error_field", "run_id"]
  labels     = local.streaming_labels
  schema = jsonencode([
    { name = "raw_payload", type = "STRING", mode = "REQUIRED" },
    { name = "error_reason", type = "STRING", mode = "REQUIRED" },
    { name = "error_field", type = "STRING", mode = "NULLABLE" },
    { name = "ingestion_timestamp", type = "TIMESTAMP", mode = "NULLABLE" },
    { name = "processing_timestamp", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "run_id", type = "STRING", mode = "REQUIRED" },
  ])
}

resource "google_bigquery_table" "streaming_pipeline_observations" {
  count = var.enable_gcp_streaming ? 1 : 0

  project             = var.project_id
  dataset_id          = google_bigquery_dataset.risk_analytics.dataset_id
  table_id            = "streaming_pipeline_observations"
  deletion_protection = false

  time_partitioning {
    type          = "DAY"
    field         = "observation_timestamp"
    expiration_ms = var.streaming_partition_retention_days * 24 * 60 * 60 * 1000
  }

  clustering = ["metric_name", "status", "severity", "run_id"]
  labels     = local.streaming_labels
  schema = jsonencode([
    { name = "observation_timestamp", type = "TIMESTAMP", mode = "REQUIRED" },
    { name = "run_id", type = "STRING", mode = "REQUIRED" },
    { name = "metric_name", type = "STRING", mode = "REQUIRED" },
    { name = "metric_value", type = "FLOAT", mode = "REQUIRED" },
    { name = "status", type = "STRING", mode = "REQUIRED" },
    { name = "severity", type = "STRING", mode = "REQUIRED" },
    { name = "details", type = "STRING", mode = "NULLABLE" },
  ])
}
