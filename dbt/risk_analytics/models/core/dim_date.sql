SELECT
    event_date,
    EXTRACT(YEAR FROM event_date) AS calendar_year,
    EXTRACT(QUARTER FROM event_date) AS calendar_quarter,
    EXTRACT(MONTH FROM event_date) AS calendar_month,
    EXTRACT(DAY FROM event_date) AS day_of_month,
    EXTRACT(DAYOFWEEK FROM event_date) AS day_of_week,
    FORMAT_DATE('%A', event_date) AS day_name,
    COUNT(*) AS transaction_count
FROM {{ ref('fct_transactions') }}
GROUP BY event_date
