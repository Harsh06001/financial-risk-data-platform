{{ config(enabled=(var('enable_streaming_models', false) or var('enable_gcp_streaming_models', false)), materialized='table') }}

select
    event_date,
    event_hour,
    event_type,
    count(*) as event_count,
    countif(amount >= 1000) as high_value_event_count,
    round(sum(amount), 2) as total_amount,
    round(avg(amount), 2) as average_amount,
    count(distinct customer_id) as unique_customers,
    max(processing_timestamp) as latest_processing_timestamp
from {{ ref('stg_streaming_transaction_events') }}
group by event_date, event_hour, event_type
