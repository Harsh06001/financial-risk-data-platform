{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='transaction_id',
        partition_by={
            'field': 'event_date',
            'data_type': 'date',
            'granularity': 'day'
        },
        on_schema_change='fail'
    )
}}

SELECT
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
    is_fraud,
    source_file,
    processed_at
FROM {{ ref('stg_processed_transactions') }}
{% if is_incremental() %}
WHERE processed_at >= (
    SELECT COALESCE(MAX(processed_at), TIMESTAMP('1900-01-01'))
    FROM {{ this }}
)
{% endif %}
