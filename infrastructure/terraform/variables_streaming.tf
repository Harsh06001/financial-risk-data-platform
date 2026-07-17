variable "enable_gcp_streaming" {
  description = "Opt in to creating Pub/Sub, Dataflow IAM, and streaming BigQuery resources"
  type        = bool
  default     = false
}

variable "enable_monitoring_alerts" {
  description = "Opt in to Cloud Monitoring policies for the GCP streaming demo"
  type        = bool
  default     = false
}

variable "enable_budget_alerts" {
  description = "Opt in to a project-scoped Cloud Billing budget; requires billing_account_id"
  type        = bool
  default     = false
}

variable "billing_account_id" {
  description = "Cloud Billing account ID used only when enable_budget_alerts is true"
  type        = string
  default     = ""
  sensitive   = true
}

variable "budget_amount_usd" {
  description = "Monthly budget amount fixed at 100 USD for 25/50/75/100 USD thresholds"
  type        = number
  default     = 100

  validation {
    condition     = var.budget_amount_usd == 100
    error_message = "budget_amount_usd must remain 100 to preserve the documented 25/50/75/100 USD thresholds."
  }
}

variable "demo_event_count" {
  description = "Guarded event count for the short GCP streaming demo"
  type        = number
  default     = 1000

  validation {
    condition     = var.demo_event_count >= 1 && var.demo_event_count <= 10000
    error_message = "demo_event_count must be between 1 and 10000."
  }
}

variable "max_demo_minutes" {
  description = "Maximum documented runtime for the unbounded short demo"
  type        = number
  default     = 15

  validation {
    condition     = var.max_demo_minutes >= 1 && var.max_demo_minutes <= 15
    error_message = "max_demo_minutes must be between 1 and 15."
  }
}

variable "dataflow_num_workers" {
  description = "Initial Dataflow worker count for the guarded demo"
  type        = number
  default     = 1

  validation {
    condition     = var.dataflow_num_workers == 1
    error_message = "The guarded demo must start with exactly one worker."
  }
}

variable "dataflow_max_workers" {
  description = "Maximum Dataflow workers allowed by the guarded demo"
  type        = number
  default     = 1

  validation {
    condition     = var.dataflow_max_workers >= 1 && var.dataflow_max_workers <= 2
    error_message = "dataflow_max_workers must be 1 or 2."
  }
}

variable "dataflow_machine_type" {
  description = "Small Dataflow worker machine type for the short portfolio demo"
  type        = string
  default     = "n1-standard-1"
}

variable "streaming_partition_retention_days" {
  description = "Automatic partition expiration for the three streaming demo tables"
  type        = number
  default     = 7

  validation {
    condition     = var.streaming_partition_retention_days >= 1 && var.streaming_partition_retention_days <= 30
    error_message = "streaming_partition_retention_days must be between 1 and 30."
  }
}

variable "streaming_notification_channel_ids" {
  description = "Optional existing Cloud Monitoring notification channel resource names"
  type        = list(string)
  default     = []
}

variable "pubsub_backlog_alert_threshold" {
  description = "Undelivered-message threshold for the demo subscription"
  type        = number
  default     = 1000
}

variable "invalid_record_rate_threshold" {
  description = "Invalid record fraction that triggers the optional monitoring policy"
  type        = number
  default     = 0.10

  validation {
    condition     = var.invalid_record_rate_threshold > 0 && var.invalid_record_rate_threshold <= 1
    error_message = "invalid_record_rate_threshold must be greater than 0 and no more than 1."
  }
}
