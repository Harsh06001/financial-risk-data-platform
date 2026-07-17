{{ config(enabled=var('enable_streaming_models', false), materialized='table') }}

select
    event_date,
    count(*) as event_count,
    countif(transaction_id is null) as null_transaction_id_count,
    countif(amount is null or amount <= 0 or amount > 1000000) as invalid_amount_count,
    countif(is_late) as late_event_count,
    count(distinct processing_batch_id) as processing_batch_count,
    count(distinct processing_run_id) as processing_run_count,
    round(avg(timestamp_diff(ingestion_timestamp, event_timestamp, second)), 2)
        as average_ingestion_delay_seconds,
    max(ingestion_timestamp) as latest_ingestion_timestamp
from {{ ref('stg_streaming_transaction_events') }}
group by event_date
