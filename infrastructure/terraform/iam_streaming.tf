resource "google_service_account" "streaming_dataflow" {
  count = var.enable_gcp_streaming ? 1 : 0

  account_id   = "risk-streaming-dataflow"
  display_name = "Financial risk streaming Dataflow demo"
  description  = "Least-scope worker identity for the opt-in short streaming demo"
  project      = var.project_id
}

resource "google_project_iam_member" "streaming_dataflow_project_roles" {
  for_each = var.enable_gcp_streaming ? toset([
    "roles/dataflow.worker",
    "roles/monitoring.metricWriter",
    "roles/pubsub.subscriber",
    "roles/pubsub.viewer",
  ]) : toset([])

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.streaming_dataflow[0].email}"
}

resource "google_bigquery_dataset_iam_member" "streaming_dataflow_editor" {
  count = var.enable_gcp_streaming ? 1 : 0

  project    = var.project_id
  dataset_id = google_bigquery_dataset.risk_analytics.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.streaming_dataflow[0].email}"
}

resource "google_storage_bucket_iam_member" "streaming_dataflow_temp_objects" {
  count = var.enable_gcp_streaming ? 1 : 0

  bucket = google_storage_bucket.streaming_dataflow_temp[0].name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.streaming_dataflow[0].email}"
}

data "google_project" "streaming_project" {
  count = var.enable_gcp_streaming ? 1 : 0

  project_id = var.project_id

  depends_on = [google_project_service.streaming_apis]
}

resource "google_pubsub_topic_iam_member" "pubsub_service_agent_dlq_publisher" {
  count = var.enable_gcp_streaming ? 1 : 0

  project = var.project_id
  topic   = google_pubsub_topic.transaction_events_dlq[0].name
  role    = "roles/pubsub.publisher"
  member  = "serviceAccount:service-${data.google_project.streaming_project[0].number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_subscription_iam_member" "pubsub_service_agent_forwarder" {
  count = var.enable_gcp_streaming ? 1 : 0

  project      = var.project_id
  subscription = google_pubsub_subscription.transaction_events_dataflow[0].name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:service-${data.google_project.streaming_project[0].number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}
