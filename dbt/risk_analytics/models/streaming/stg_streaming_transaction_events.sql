{{ config(enabled=var('enable_streaming_models', false), materialized='view') }}

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
    is_late,
    processing_batch_id,
    processing_run_id
from {{ source('risk_analytics', 'streaming_transaction_events') }}
where transaction_id is not null
  and amount > 0
