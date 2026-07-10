# Risk Feature Engineering

`build_risk_features.py` reads validated processed Parquet once and constructs five datasets:

- Daily: transaction/amount/fraud/high-value statistics by date.
- Customer: volume, amount, fraud, diversity, active days, and activity window.
- Merchant: analogous current-state merchant behavior.
- Segment: country/category/payment-method aggregates.
- High risk: transaction detail where fraud is true or amount is at least 1,000, with one of three deterministic reasons.

Helper expressions keep fraud count/rate and high-value logic consistent. `current_timestamp` records feature generation. Tables are overwritten because they summarize the current complete processed snapshot; only the transaction fact is made incremental in v1.1.

Feature tables support risk-oriented marts but are not claimed as predictive machine-learning features. At higher scale, compute only affected aggregates or use partition-aware incremental aggregation, while retaining reconciliation against the transaction fact.
