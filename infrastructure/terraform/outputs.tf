output "raw_data_bucket_name" {
  description = "Name of the raw data GCS bucket"
  value       = google_storage_bucket.raw_data.name
}

output "raw_data_bucket_url" {
  description = "GCS URL of the raw data bucket"
  value       = google_storage_bucket.raw_data.url
}
