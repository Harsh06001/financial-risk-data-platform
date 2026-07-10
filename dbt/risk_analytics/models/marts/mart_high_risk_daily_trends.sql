SELECT
    event_date,
    COUNT(*) AS high_risk_transaction_count,
    SUM(CASE WHEN is_fraud THEN 1 ELSE 0 END) AS known_fraud_count,
    SUM(CASE WHEN NOT is_fraud AND amount >= 1000 THEN 1 ELSE 0 END) AS high_value_only_count,
    SUM(CASE WHEN is_fraud AND amount >= 1000 THEN 1 ELSE 0 END) AS combined_fraud_and_high_value_count,
    SUM(amount) AS total_high_risk_amount
FROM {{ ref('stg_high_risk_transactions') }}
GROUP BY event_date
ORDER BY event_date
