{{ config(enabled=(var('enable_streaming_models', false) or var('enable_gcp_streaming_models', false))) }}

with staged as (
    select event_date, count(*) as expected_event_count
    from {{ ref('stg_streaming_transaction_events') }}
    group by event_date
),
quality as (
    select event_date, event_count
    from {{ ref('mart_streaming_event_quality') }}
)
select
    coalesce(staged.event_date, quality.event_date) as event_date,
    staged.expected_event_count,
    quality.event_count
from staged
full outer join quality using (event_date)
where staged.expected_event_count is distinct from quality.event_count
