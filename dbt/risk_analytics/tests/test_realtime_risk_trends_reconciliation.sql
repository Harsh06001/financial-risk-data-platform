{{ config(enabled=(var('enable_streaming_models', false) or var('enable_gcp_streaming_models', false))) }}

with staging as (
    select event_date, event_hour, event_type, count(*) as event_count
    from {{ ref('stg_streaming_transaction_events') }}
    group by event_date, event_hour, event_type
),
mart as (
    select event_date, event_hour, event_type, event_count
    from {{ ref('mart_realtime_risk_trends') }}
)
select
    coalesce(staging.event_date, mart.event_date) as event_date,
    coalesce(staging.event_hour, mart.event_hour) as event_hour,
    coalesce(staging.event_type, mart.event_type) as event_type
from staging
full outer join mart
    using (event_date, event_hour, event_type)
where coalesce(staging.event_count, -1) != coalesce(mart.event_count, -1)
