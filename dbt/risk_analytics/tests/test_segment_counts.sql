SELECT 1 AS failure
FROM (
  SELECT SUM(transaction_count) AS total_transactions
  FROM {{ ref('stg_segment_risk_summary') }}
) AS segment_counts
WHERE COALESCE(total_transactions, 0) != 100350
