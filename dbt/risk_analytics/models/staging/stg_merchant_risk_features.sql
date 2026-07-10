SELECT
    merchant_id,
    transaction_count,
    total_amount,
    avg_amount,
    max_amount,
    fraud_count,
    fraud_rate,
    high_value_transaction_count,
    distinct_customers,
    distinct_categories,
    distinct_countries,
    distinct_payment_methods,
    active_days,
    first_seen_at,
    last_seen_at,
    feature_generated_at
FROM {{ source('risk_analytics', 'merchant_risk_features') }}
