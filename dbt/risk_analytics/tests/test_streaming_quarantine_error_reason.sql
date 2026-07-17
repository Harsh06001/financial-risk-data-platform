{{ config(enabled=var('enable_gcp_streaming_models', false)) }}

select raw_payload
from {{ source('risk_analytics', 'streaming_transaction_events_quarantine') }}
where error_reason is null or trim(error_reason) = ''
