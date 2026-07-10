SELECT
    country,
    merchant_category,
    payment_method,
    COUNT(*) AS duplicate_count
FROM {{ ref('stg_segment_risk_summary') }}
GROUP BY country, merchant_category, payment_method
HAVING COUNT(*) > 1
