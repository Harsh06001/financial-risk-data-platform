SELECT
    source_counts.source_rows,
    fact_counts.fact_rows
FROM (
    SELECT COUNT(*) AS source_rows
    FROM {{ ref('stg_processed_transactions') }}
) AS source_counts
CROSS JOIN (
    SELECT COUNT(*) AS fact_rows
    FROM {{ ref('fct_transactions') }}
) AS fact_counts
WHERE source_counts.source_rows != fact_counts.fact_rows
