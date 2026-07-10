SELECT
    country,
    merchant_category,
    payment_method,
    transaction_count,
    total_amount,
    fraud_count,
    fraud_rate,
    high_value_transaction_count,
    distinct_customers,
    distinct_merchants,
    PERCENT_RANK() OVER (ORDER BY fraud_rate ASC, transaction_count ASC) AS risk_percentile
FROM {{ ref('stg_segment_risk_summary') }}
ORDER BY fraud_rate DESC, transaction_count DESC
