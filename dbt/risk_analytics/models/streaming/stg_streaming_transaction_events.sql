{{ config(enabled=(var('enable_streaming_models', false) or var('enable_gcp_streaming_models', false)), materialized='view') }}

select
    transaction_id,
    event_timestamp,
    event_date,
    event_hour,
    customer_id,
    merchant_id,
    amount,
    currency,
    country,
    merchant_category,
    payment_method,
    device_id,
    event_type,
    ingestion_timestamp,
    {% if var('enable_gcp_streaming_models', false) %}
    processing_timestamp,
    timestamp_diff(ingestion_timestamp, event_timestamp, hour) > 24 as is_late,
    cast(null as int64) as processing_batch_id,
    run_id as processing_run_id,
    run_id,
    source_system,
    validation_status
    {% else %}
    is_late,
    processing_batch_id,
    processing_run_id,
    cast(processing_run_id as string) as run_id,
    'local_redpanda_spark' as source_system,
    'valid' as validation_status,
    ingestion_timestamp as processing_timestamp
    {% endif %}
from {{ source('risk_analytics', 'streaming_transaction_events') }}
where transaction_id is not null
  and amount > 0
