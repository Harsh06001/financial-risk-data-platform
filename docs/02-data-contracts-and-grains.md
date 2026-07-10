# Data Contracts and Grains

The raw schema contains transaction ID, timestamp, customer, merchant, amount, currency, country, category, payment method, device, and fraud flag. Processing derives `event_date`, `event_hour`, `source_file`, and `processed_at`.

Canonical contracts:

| Dataset | Grain | Verified rows |
|---|---|---:|
| processed_transactions | transaction_id | 100,350 |
| daily_transaction_summary | event_date | 31 |
| customer_risk_features | customer_id | 500 |
| merchant_risk_features | merchant_id | 100 |
| segment_risk_summary | country + category + payment method | 140 |
| high_risk_transactions | transaction_id | 3,289 |
| fct_transactions | transaction_id | reconciles to processed_transactions |
| dim_customer | customer_id | 500 |
| dim_merchant | merchant_id | 100 |
| dim_date | observed event_date | 31 |

Required fields are checked for nulls; transaction IDs are checked for uniqueness; amounts must be positive; hours must be 0–23. Fraud rates must be non-null and between zero and one. Daily and segment counts must both reconcile to 100,350.

`event_date` is both a business grouping field and physical partition key. Hive-style directory names restore it when Parquet payload files omit the partition column.
