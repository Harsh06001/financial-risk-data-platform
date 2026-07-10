SELECT
    merchant_id,
    MIN(event_timestamp) AS first_seen_at,
    MAX(event_timestamp) AS last_seen_at,
    COUNT(*) AS transaction_count,
    COUNT(DISTINCT customer_id) AS distinct_customers,
    COUNT(DISTINCT merchant_category) AS distinct_categories,
    COUNT(DISTINCT country) AS distinct_countries
FROM {{ ref('fct_transactions') }}
GROUP BY merchant_id
