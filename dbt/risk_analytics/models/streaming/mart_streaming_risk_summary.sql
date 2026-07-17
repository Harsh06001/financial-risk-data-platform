{{ config(enabled=var('enable_streaming_models', false), materialized='table') }}

select
    event_date,
    count(*) as event_count,
    countif(is_late) as late_event_count,
    countif(amount >= 1000) as high_value_event_count,
    round(sum(amount), 2) as total_amount,
    round(avg(amount), 2) as average_amount,
    count(distinct customer_id) as unique_customers,
    count(distinct merchant_id) as unique_merchants
from {{ ref('stg_streaming_transaction_events') }}
group by event_date
