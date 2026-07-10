SELECT transaction_id
FROM {{ ref('stg_high_risk_transactions') }}
WHERE
    transaction_id IS NULL
    OR is_fraud IS NULL
    OR amount IS NULL
    OR NOT (is_fraud OR amount >= 1000.0)
    OR risk_reason IS NULL
    OR risk_reason NOT IN (
        'known_fraud_and_high_value',
        'known_fraud',
        'high_value'
    )
    OR (
        risk_reason = 'known_fraud_and_high_value'
        AND (NOT is_fraud OR amount < 1000.0)
    )
    OR (
        risk_reason = 'known_fraud'
        AND (NOT is_fraud OR amount >= 1000.0)
    )
    OR (
        risk_reason = 'high_value'
        AND (is_fraud OR amount < 1000.0)
    )
