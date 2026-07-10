SELECT
    customer_id,
    transaction_count,
    total_amount,
    fraud_count,
    fraud_rate,
    high_value_transaction_count,
    active_days,
    PERCENT_RANK() OVER (ORDER BY fraud_rate ASC, transaction_count ASC) AS risk_percentile,
    CASE
        WHEN fraud_rate >= 0.1 THEN 'high'
        WHEN fraud_rate >= 0.05 THEN 'medium'
        ELSE 'low'
    END AS risk_tier
FROM {{ ref('stg_customer_risk_features') }}
ORDER BY fraud_rate DESC, transaction_count DESC
