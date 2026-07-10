output "raw_data_bucket_name" {
  description = "Name of the raw data GCS bucket"
  value       = google_storage_bucket.raw_data.name
}

output "raw_data_bucket_url" {
  description = "GCS URL of the raw data bucket"
  value       = google_storage_bucket.raw_data.url
}

output "processed_data_bucket_name" {
  description = "Name of the processed data GCS bucket"
  value       = google_storage_bucket.processed_data.name
}

output "processed_data_bucket_url" {
  description = "GCS URL of the processed data bucket"
  value       = google_storage_bucket.processed_data.url
}

output "analytics_data_bucket_name" {
  description = "Name of the analytics data GCS bucket"
  value       = google_storage_bucket.analytics_data.name
}

output "analytics_data_bucket_url" {
  description = "GCS URL of the analytics data bucket"
  value       = google_storage_bucket.analytics_data.url
}

output "risk_analytics_dataset_id" {
  description = "ID of the BigQuery analytics dataset"
  value       = google_bigquery_dataset.risk_analytics.dataset_id
}

output "risk_analytics_dataset_location" {
  description = "Location of the BigQuery analytics dataset"
  value       = google_bigquery_dataset.risk_analytics.location
}
output "daily_transaction_summary_external_table_id" {
  description = "ID of the external daily transaction summary table"
  value       = google_bigquery_table.daily_transaction_summary_external.table_id
}