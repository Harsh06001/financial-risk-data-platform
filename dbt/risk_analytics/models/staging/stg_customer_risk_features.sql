SELECT
    customer_id,
    transaction_count,
    total_amount,
    avg_amount,
    max_amount,
    fraud_count,
    fraud_rate,
    high_value_transaction_count,
    distinct_merchants,
    distinct_categories,
    distinct_countries,
    distinct_payment_methods,
    active_days,
    first_seen_at,
    last_seen_at,
    feature_generated_at
FROM {{ source('risk_analytics', 'customer_risk_features') }}
