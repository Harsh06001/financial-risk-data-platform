WITH daily AS (
    SELECT
        event_date,
        transaction_count,
        total_amount,
        avg_amount,
        fraud_count,
        fraud_rate,
        high_value_transaction_count
    FROM {{ ref('stg_daily_transaction_summary') }}
),
lagged AS (
    SELECT
        event_date,
        transaction_count,
        total_amount,
        avg_amount,
        fraud_count,
        fraud_rate,
        high_value_transaction_count,
        LAG(transaction_count) OVER (ORDER BY event_date) AS prev_transaction_count,
        LAG(fraud_count) OVER (ORDER BY event_date) AS prev_fraud_count
    FROM daily
)
SELECT
    event_date,
    transaction_count,
    total_amount,
    avg_amount,
    fraud_count,
    fraud_rate,
    high_value_transaction_count,
    CASE
        WHEN prev_transaction_count IS NULL THEN NULL
        ELSE transaction_count - prev_transaction_count
    END AS transaction_count_change,
    CASE
        WHEN prev_fraud_count IS NULL THEN NULL
        ELSE fraud_count - prev_fraud_count
    END AS fraud_count_change
FROM lagged
ORDER BY event_date
