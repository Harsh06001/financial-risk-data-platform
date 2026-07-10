SELECT
    country,
    merchant_category,
    payment_method,
    transaction_count,
    total_amount,
    avg_amount,
    max_amount,
    fraud_count,
    fraud_rate,
    high_value_transaction_count,
    distinct_customers,
    distinct_merchants,
    feature_generated_at
FROM {{ source('risk_analytics', 'segment_risk_summary') }}
