SELECT CAST(event_date AS STRING) AS record_key
FROM {{ ref('stg_daily_transaction_summary') }}
WHERE fraud_rate IS NULL OR fraud_rate < 0 OR fraud_rate > 1

UNION ALL

SELECT customer_id AS record_key
FROM {{ ref('stg_customer_risk_features') }}
WHERE fraud_rate IS NULL OR fraud_rate < 0 OR fraud_rate > 1

UNION ALL

SELECT merchant_id AS record_key
FROM {{ ref('stg_merchant_risk_features') }}
WHERE fraud_rate IS NULL OR fraud_rate < 0 OR fraud_rate > 1

UNION ALL

SELECT CONCAT(country, '|', merchant_category, '|', payment_method) AS record_key
FROM {{ ref('stg_segment_risk_summary') }}
WHERE fraud_rate IS NULL OR fraud_rate < 0 OR fraud_rate > 1
