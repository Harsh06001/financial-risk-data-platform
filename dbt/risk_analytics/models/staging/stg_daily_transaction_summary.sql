SELECT
    event_date,
    transaction_count,
    total_amount,
    avg_amount,
    min_amount,
    max_amount,
    fraud_count,
    fraud_rate,
    high_value_transaction_count,
    feature_generated_at
FROM {{ source('risk_analytics', 'daily_transaction_summary') }}
