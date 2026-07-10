SELECT
    COALESCE(staging.event_date, mart.event_date) AS event_date
FROM {{ ref('stg_daily_transaction_summary') }} AS staging
FULL OUTER JOIN {{ ref('mart_daily_risk_kpis') }} AS mart
    ON staging.event_date = mart.event_date
WHERE
    staging.event_date IS NULL
    OR mart.event_date IS NULL
    OR staging.transaction_count IS DISTINCT FROM mart.transaction_count
    OR staging.total_amount IS DISTINCT FROM mart.total_amount
    OR staging.avg_amount IS DISTINCT FROM mart.avg_amount
    OR staging.fraud_count IS DISTINCT FROM mart.fraud_count
    OR staging.fraud_rate IS DISTINCT FROM mart.fraud_rate
    OR staging.high_value_transaction_count
        IS DISTINCT FROM mart.high_value_transaction_count
