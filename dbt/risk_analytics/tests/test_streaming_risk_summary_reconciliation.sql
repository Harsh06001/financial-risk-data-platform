{{ config(enabled=(var('enable_streaming_models', false) or var('enable_gcp_streaming_models', false))) }}

select summary.event_date, summary.event_count
from {{ ref('mart_streaming_risk_summary') }} summary
where event_count <= 0
   or event_count != (
       select count(*)
       from {{ ref('stg_streaming_transaction_events') }} staged
       where staged.event_date = summary.event_date
   )
