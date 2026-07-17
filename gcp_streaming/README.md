# Cost-Controlled GCP Streaming Mode

This directory contains the optional v1.4 Pub/Sub → Apache Beam/Dataflow → BigQuery path. It does not replace the verified batch pipeline or local Redpanda/Spark path. Live GCP use is disabled by default and was not performed merely by adding these files.

## Safe local checks

```bash
.venv-v12/bin/python -m pytest gcp_streaming/tests -q
make gcp-streaming-producer-dry-run
make gcp-streaming-dbt-parse
sh -n gcp_streaming/scripts/*.sh
```

## Live safety sequence

Only after explicit approval and reviewed provisioning:

```bash
export GCP_PROJECT_ID="your-project-id"
export ACKNOWLEDGE_GCP_COST_RISK=true
make gcp-streaming-preflight
make gcp-streaming-plan
make gcp-streaming-demo
make gcp-streaming-stop
make gcp-streaming-check-active
```

Defaults are 1,000 events, 15 minutes, one initial worker, one maximum worker, and `n1-standard-1`. Pub/Sub is unbounded, so cancellation and a final active-job check are mandatory. These safeguards reduce risk but do not guarantee cost.

## Layout

- `producer/`: deterministic dry-run/live Pub/Sub publisher with event-count and acknowledgement guards.
- `beam_pipeline/`: explicit schemas, pure validation/routing helpers, and the Dataflow pipeline.
- `scripts/`: cost preflight, short launcher, narrowly matched stop, cleanup, and active-resource inventory.
- `tests/`: credential-free unit/configuration tests.

Read [GCP deployment](../docs/28-gcp-streaming-deployment.md), [cost controls and cleanup](../docs/29-gcp-cost-controls-and-cleanup.md), and the [runbook](../docs/30-gcp-streaming-runbook.md) before any live command.
