# Terraform and GCP Infrastructure

`infrastructure/terraform/` declares three regional GCS buckets, the `risk_analytics` BigQuery dataset, and `daily_transaction_summary_external`. Variables define project `risk-data-platform-npg-2026` and region `us-central1`; outputs expose bucket names/URLs and dataset/table identifiers.

The external table demonstrates reading GCS Parquet directly. Native warehouse tables are created by load scripts because their schemas originate from validated Parquet and are refreshed operationally.

Commands:

```bash
cd infrastructure/terraform
terraform fmt -check
terraform validate
terraform plan
```

No documentation command implies `apply` or `destroy`. Current development resources use permissive deletion settings; this is a conscious sandbox trade-off, not a production recommendation. Production should use remote encrypted state, reviewable plans, deletion protection, versioning/soft delete, least-privilege service accounts, separate environments, and policy checks.

At 1 GB the current layout is adequate. At 1 TB, lifecycle policies, remote state, and workload-specific IAM become important. At 100 TB, storage classes, retention policy, organization policies, centralized logging, and cost governance become first-class design requirements.
