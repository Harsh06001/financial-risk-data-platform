{{ config(enabled=var('enable_streaming_models', false)) }}

select transaction_id
from {{ ref('stg_streaming_transaction_events') }}
where amount <= 0
   or amount > 1000000
