locals {
  streaming_labels = {
    project     = "financial-risk-data-platform"
    environment = "demo"
    component   = "gcp-streaming"
    cost_scope  = "short-demo"
  }
}

resource "google_project_service" "streaming_apis" {
  for_each = var.enable_gcp_streaming ? toset([
    "bigquery.googleapis.com",
    "compute.googleapis.com",
    "dataflow.googleapis.com",
    "iam.googleapis.com",
    "monitoring.googleapis.com",
    "pubsub.googleapis.com",
  ]) : toset([])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_project_service" "billing_budget_api" {
  count = var.enable_budget_alerts ? 1 : 0

  project            = var.project_id
  service            = "billingbudgets.googleapis.com"
  disable_on_destroy = false
}

resource "google_pubsub_topic" "transaction_events" {
  count = var.enable_gcp_streaming ? 1 : 0

  name                       = "transaction-events"
  message_retention_duration = "86400s"
  labels                     = local.streaming_labels

  depends_on = [google_project_service.streaming_apis]
}

resource "google_pubsub_topic" "transaction_events_dlq" {
  count = var.enable_gcp_streaming ? 1 : 0

  name                       = "transaction-events-dlq"
  message_retention_duration = "86400s"
  labels                     = local.streaming_labels

  depends_on = [google_project_service.streaming_apis]
}

resource "google_pubsub_subscription" "transaction_events_dataflow" {
  count = var.enable_gcp_streaming ? 1 : 0

  name                       = "transaction-events-dataflow-sub"
  topic                      = google_pubsub_topic.transaction_events[0].id
  ack_deadline_seconds       = 60
  message_retention_duration = "86400s"
  retain_acked_messages      = false
  labels                     = local.streaming_labels

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.transaction_events_dlq[0].id
    max_delivery_attempts = 5
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "60s"
  }
}
