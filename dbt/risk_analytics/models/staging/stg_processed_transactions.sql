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
FROM {{ source('risk_analytics', 'processed_transactions') }}
