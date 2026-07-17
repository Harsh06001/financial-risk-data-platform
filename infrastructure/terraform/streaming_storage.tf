resource "google_storage_bucket" "streaming_dataflow_temp" {
  count = var.enable_gcp_streaming ? 1 : 0

  name          = "${var.project_id}-streaming-dataflow-temp"
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

  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "Delete"
    }
  }

  labels = local.streaming_labels

  depends_on = [google_project_service.streaming_apis]
}
