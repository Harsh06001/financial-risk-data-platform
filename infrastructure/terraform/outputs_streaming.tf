output "gcp_streaming_enabled" {
  description = "Whether opt-in GCP streaming resources are enabled"
  value       = var.enable_gcp_streaming
}

output "streaming_topic_id" {
  description = "Pub/Sub transaction topic when GCP streaming is enabled"
  value       = try(google_pubsub_topic.transaction_events[0].id, null)
}

output "streaming_subscription_id" {
  description = "Dataflow subscription when GCP streaming is enabled"
  value       = try(google_pubsub_subscription.transaction_events_dataflow[0].id, null)
}

output "streaming_dead_letter_topic_id" {
  description = "Dead-letter Pub/Sub topic when GCP streaming is enabled"
  value       = try(google_pubsub_topic.transaction_events_dlq[0].id, null)
}

output "streaming_dataflow_service_account_email" {
  description = "Worker service account for the opt-in Dataflow demo"
  value       = try(google_service_account.streaming_dataflow[0].email, null)
}

output "streaming_dataflow_temp_bucket_name" {
  description = "Dedicated one-day-lifecycle bucket for Dataflow staging and temp objects"
  value       = try(google_storage_bucket.streaming_dataflow_temp[0].name, null)
}

output "streaming_bigquery_table_ids" {
  description = "BigQuery tables created by the opt-in GCP streaming mode"
  value = var.enable_gcp_streaming ? [
    google_bigquery_table.streaming_transaction_events[0].id,
    google_bigquery_table.streaming_transaction_events_quarantine[0].id,
    google_bigquery_table.streaming_pipeline_observations[0].id,
  ] : []
}

output "gcp_streaming_guardrails" {
  description = "Configured demo limits; these are safeguards, not a cost guarantee"
  value = {
    demo_event_count  = var.demo_event_count
    max_demo_minutes  = var.max_demo_minutes
    num_workers       = var.dataflow_num_workers
    max_workers       = var.dataflow_max_workers
    machine_type      = var.dataflow_machine_type
    partition_days    = var.streaming_partition_retention_days
    monitoring_alerts = var.enable_monitoring_alerts
    budget_alerts     = var.enable_budget_alerts
  }
}
