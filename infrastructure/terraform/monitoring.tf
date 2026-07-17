resource "google_monitoring_alert_policy" "pubsub_backlog_high" {
  count = var.enable_gcp_streaming && var.enable_monitoring_alerts ? 1 : 0

  display_name          = "GCP streaming demo: Pub/Sub backlog high"
  combiner              = "OR"
  notification_channels = var.streaming_notification_channel_ids
  user_labels           = local.streaming_labels

  conditions {
    display_name = "Undelivered demo messages exceed guarded threshold"
    condition_threshold {
      filter = join(" AND ", [
        "resource.type=\"pubsub_subscription\"",
        "metric.type=\"pubsub.googleapis.com/subscription/num_undelivered_messages\"",
        "resource.label.subscription_id=\"transaction-events-dataflow-sub\"",
      ])
      comparison      = "COMPARISON_GT"
      threshold_value = var.pubsub_backlog_alert_threshold
      duration        = "300s"

      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MAX"
      }
    }
  }

  documentation {
    content   = "Stop the demo if needed, then inspect the subscription and Dataflow worker. See docs/30-gcp-streaming-runbook.md."
    mime_type = "text/markdown"
  }
}

resource "google_monitoring_alert_policy" "dataflow_job_failure" {
  count = var.enable_gcp_streaming && var.enable_monitoring_alerts ? 1 : 0

  display_name          = "GCP streaming demo: Dataflow job failed"
  combiner              = "OR"
  notification_channels = var.streaming_notification_channel_ids
  user_labels           = local.streaming_labels

  conditions {
    display_name = "Dataflow job reports failure"
    condition_threshold {
      filter          = "resource.type=\"dataflow_job\" AND metric.type=\"dataflow.googleapis.com/job/is_failed\" AND resource.label.job_name=monitoring.regex.full_match(\"financial-risk-v14-demo-.*\")"
      comparison      = "COMPARISON_GT"
      threshold_value = 0
      duration        = "0s"

      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MAX"
      }
    }
  }
}

resource "google_monitoring_alert_policy" "no_recent_streaming_rows" {
  count = var.enable_gcp_streaming && var.enable_monitoring_alerts ? 1 : 0

  display_name          = "GCP streaming demo: no recent valid rows"
  combiner              = "OR"
  notification_channels = var.streaming_notification_channel_ids
  user_labels           = local.streaming_labels

  conditions {
    display_name = "Valid-record counter absent for ten minutes"
    condition_prometheus_query_language {
      query               = <<-EOT
        absent_over_time({"__name__"="dataflow.googleapis.com/job/user_counter", "metric_name"="valid_records", "job_name"=~"financial-risk-v14-demo-.*"}[10m]) == 1
        and on()
        sum(max_over_time({"__name__"="dataflow.googleapis.com/job/current_num_vcpus", "job_name"=~"financial-risk-v14-demo-.*"}[5m])) > 0
      EOT
      duration            = "60s"
      evaluation_interval = "60s"
    }
  }
}

resource "google_monitoring_alert_policy" "invalid_record_rate_high" {
  count = var.enable_gcp_streaming && var.enable_monitoring_alerts ? 1 : 0

  display_name          = "GCP streaming demo: invalid record rate high"
  combiner              = "OR"
  notification_channels = var.streaming_notification_channel_ids
  user_labels           = local.streaming_labels

  conditions {
    display_name = "Invalid records exceed configured fraction"
    condition_prometheus_query_language {
      query               = <<-EOT
        sum(delta({"__name__"="dataflow.googleapis.com/job/user_counter", "metric_name"="invalid_records", "job_name"=~"financial-risk-v14-demo-.*"}[5m]))
        /
        clamp_min(
          sum(delta({"__name__"="dataflow.googleapis.com/job/user_counter", "metric_name"=~"valid_records|invalid_records", "job_name"=~"financial-risk-v14-demo-.*"}[5m])),
          1
        ) > ${var.invalid_record_rate_threshold}
      EOT
      duration            = "300s"
      evaluation_interval = "60s"
    }
  }
}

resource "google_monitoring_alert_policy" "bigquery_write_failure" {
  count = var.enable_gcp_streaming && var.enable_monitoring_alerts ? 1 : 0

  display_name          = "GCP streaming demo: BigQuery write or validation failure"
  combiner              = "OR"
  notification_channels = var.streaming_notification_channel_ids
  user_labels           = local.streaming_labels

  conditions {
    display_name = "BigQuery write-failure user counter increased"
    condition_prometheus_query_language {
      query               = <<-EOT
        sum(delta({"__name__"="dataflow.googleapis.com/job/user_counter", "metric_name"="bigquery_write_failures", "job_name"=~"financial-risk-v14-demo-.*"}[5m])) > 0
      EOT
      duration            = "0s"
      evaluation_interval = "60s"
    }
  }
}

resource "google_monitoring_alert_policy" "dataflow_demo_overrun" {
  count = var.enable_gcp_streaming && var.enable_monitoring_alerts ? 1 : 0

  display_name          = "GCP streaming demo: runtime exceeds safety window"
  combiner              = "OR"
  notification_channels = var.streaming_notification_channel_ids
  user_labels           = local.streaming_labels

  conditions {
    display_name = "Dataflow elapsed runtime exceeds configured demo minutes"
    condition_prometheus_query_language {
      query               = <<-EOT
        max({"__name__"="dataflow.googleapis.com/job/elapsed_time", "job_name"=~"financial-risk-v14-demo-.*"}) > ${var.max_demo_minutes * 60}
        and on()
        sum({"__name__"="dataflow.googleapis.com/job/current_num_vcpus", "job_name"=~"financial-risk-v14-demo-.*"}) > 0
      EOT
      duration            = "60s"
      evaluation_interval = "60s"
    }
  }
}

data "google_project" "budget_project" {
  count = var.enable_budget_alerts ? 1 : 0

  project_id = var.project_id
}

resource "google_billing_budget" "streaming_demo" {
  count = var.enable_budget_alerts ? 1 : 0

  billing_account = var.billing_account_id
  display_name    = "Financial risk streaming demo cost guard"

  budget_filter {
    projects = ["projects/${data.google_project.budget_project[0].number}"]
  }

  amount {
    specified_amount {
      currency_code = "USD"
      units         = tostring(var.budget_amount_usd)
    }
  }

  dynamic "threshold_rules" {
    for_each = toset([0.25, 0.50, 0.75, 1.00])
    content {
      threshold_percent = threshold_rules.value
      spend_basis       = "CURRENT_SPEND"
    }
  }

  lifecycle {
    precondition {
      condition     = trimspace(var.billing_account_id) != ""
      error_message = "billing_account_id is required when enable_budget_alerts=true."
    }
  }

  depends_on = [google_project_service.billing_budget_api]
}
