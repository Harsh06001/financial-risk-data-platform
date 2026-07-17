{{ config(enabled=(var('enable_streaming_models', false) or var('enable_gcp_streaming_models', false))) }}

select transaction_id
from {{ ref('stg_streaming_transaction_events') }}
where event_timestamp > ingestion_timestamp
   or ingestion_timestamp > processing_timestamp
