SELECT
    customer_id,
    MIN(event_timestamp) AS first_seen_at,
    MAX(event_timestamp) AS last_seen_at,
    COUNT(*) AS transaction_count,
    COUNT(DISTINCT country) AS distinct_countries,
    COUNT(DISTINCT merchant_id) AS distinct_merchants
FROM {{ ref('fct_transactions') }}
GROUP BY customer_id
