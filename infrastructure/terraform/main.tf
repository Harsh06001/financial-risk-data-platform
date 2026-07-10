resource "google_storage_bucket" "raw_data" {
  name          = "${var.project_id}-raw-data"
  location      = var.region
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  force_destroy               = true

  versioning {
    enabled = false
  }

  soft_delete_policy {
    retention_duration_seconds = 0
  }

  labels = {
    project     = "financial-risk-data-platform"
    environment = "development"
    layer       = "raw"
  }
}
resource "google_storage_bucket" "processed_data" {
  name          = "${var.project_id}-processed-data"
  location      = var.region
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  force_destroy               = true

  versioning {
    enabled = false
  }

  soft_delete_policy {
    retention_duration_seconds = 0
  }

  labels = {
    project     = "financial-risk-data-platform"
    environment = "development"
    layer       = "processed"
  }
}

resource "google_storage_bucket" "analytics_data" {
  name          = "${var.project_id}-analytics-data"
  location      = var.region
  storage_class = "STANDARD"

  uniform_bucket_level_access = true
  force_destroy               = true

  versioning {
    enabled = false
  }

  soft_delete_policy {
    retention_duration_seconds = 0
  }

  labels = {
    project     = "financial-risk-data-platform"
    environment = "development"
    layer       = "analytics"
  }
}

resource "google_bigquery_dataset" "risk_analytics" {
  dataset_id = "risk_analytics"
  location   = var.region

  friendly_name = "Financial Risk Analytics"

  description = "Validated analytics and risk feature tables for the financial risk data platform."

  delete_contents_on_destroy = true

  labels = {
    project     = "financial-risk-data-platform"
    environment = "development"
    layer       = "analytics"
  }
}

resource "google_bigquery_table" "daily_transaction_summary_external" {
  dataset_id = google_bigquery_dataset.risk_analytics.dataset_id
  table_id   = "daily_transaction_summary_external"

  deletion_protection = false

  external_data_configuration {
    autodetect    = true
    source_format = "PARQUET"

    source_uris = [
      "gs://${google_storage_bucket.analytics_data.name}/risk_features/daily_transaction_summary/*.parquet"
    ]
  }

  labels = {
    project     = "financial-risk-data-platform"
    environment = "development"
    layer       = "analytics"
    table_type  = "external"
  }
}