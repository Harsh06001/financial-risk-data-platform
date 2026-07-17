"""Pub/Sub to BigQuery Apache Beam pipeline for the opt-in GCP demo.

The pipeline is unbounded because Pub/Sub is unbounded. The guarded demo launcher
submits it with one worker and stops it after the configured maximum duration.
"""

import argparse
import logging
import os
import uuid

from gcp_streaming.beam_pipeline.schemas import (
    OBSERVATION_SCHEMA,
    QUARANTINE_SCHEMA,
    VALID_EVENT_SCHEMA,
)
from gcp_streaming.beam_pipeline.validation import (
    bigquery_failure_rows,
    route_payload,
)


def table_spec(project: str, dataset: str, table: str) -> str:
    return f"{project}:{dataset}.{table}"


def parse_arguments(arguments: list[str] | None = None) -> tuple[argparse.Namespace, list[str]]:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project", required=True)
    parser.add_argument("--region", default="us-central1")
    parser.add_argument("--subscription", required=True)
    parser.add_argument("--dataset", default="risk_analytics")
    parser.add_argument("--temp_location", required=True)
    parser.add_argument("--staging_location", required=True)
    parser.add_argument("--job_name", default=f"financial-risk-v14-demo-{uuid.uuid4().hex[:8]}")
    parser.add_argument("--service_account_email", required=True)
    parser.add_argument("--num_workers", type=int, default=1)
    parser.add_argument("--max_num_workers", type=int, default=1)
    parser.add_argument("--machine_type", default="n1-standard-1")
    parser.add_argument("--run_id", default=os.environ.get("GCP_STREAMING_RUN_ID", uuid.uuid4().hex))
    return parser.parse_known_args(arguments)


def validate_worker_limits(num_workers: int, max_num_workers: int) -> None:
    if num_workers != 1:
        raise ValueError("guarded demo requires exactly one initial worker")
    if not 1 <= max_num_workers <= 2:
        raise ValueError("guarded demo allows at most two workers")
    if num_workers > max_num_workers:
        raise ValueError("initial workers cannot exceed maximum workers")


def build_pipeline(options: argparse.Namespace, beam_args: list[str]):
    try:
        import apache_beam as beam
        from apache_beam.io.gcp.bigquery import BigQueryDisposition, RetryStrategy
        from apache_beam.metrics import Metrics
        from apache_beam.options.pipeline_options import (
            GoogleCloudOptions,
            PipelineOptions,
            SetupOptions,
            StandardOptions,
            WorkerOptions,
        )
    except ImportError as exc:
        raise RuntimeError(
            "Apache Beam dependency is missing; install requirements-gcp-streaming.txt"
        ) from exc

    validate_worker_limits(options.num_workers, options.max_num_workers)

    class ParseAndRoute(beam.DoFn):
        def __init__(self, run_id: str):
            self.run_id = run_id
            self.valid_counter = Metrics.counter("financial_risk_streaming", "valid_records")
            self.invalid_counter = Metrics.counter("financial_risk_streaming", "invalid_records")

        def process(self, payload):
            route, row, observation = route_payload(payload, self.run_id)
            if route == "valid":
                self.valid_counter.inc()
                yield row
            else:
                self.invalid_counter.inc()
                yield beam.pvalue.TaggedOutput("invalid", row)
            yield beam.pvalue.TaggedOutput("observation", observation)

    class RouteWriteFailure(beam.DoFn):
        def __init__(self, run_id: str):
            self.run_id = run_id
            self.failure_counter = Metrics.counter(
                "financial_risk_streaming", "bigquery_write_failures"
            )

        def process(self, failed):
            self.failure_counter.inc()
            quarantine, observation = bigquery_failure_rows(failed, self.run_id)
            yield quarantine
            yield beam.pvalue.TaggedOutput("observation", observation)

    pipeline_options = PipelineOptions(beam_args)
    pipeline_options.view_as(StandardOptions).streaming = True
    pipeline_options.view_as(SetupOptions).save_main_session = True
    cloud = pipeline_options.view_as(GoogleCloudOptions)
    cloud.project = options.project
    cloud.region = options.region
    cloud.temp_location = options.temp_location
    cloud.staging_location = options.staging_location
    cloud.job_name = options.job_name
    cloud.service_account_email = options.service_account_email
    workers = pipeline_options.view_as(WorkerOptions)
    workers.num_workers = options.num_workers
    workers.max_num_workers = options.max_num_workers
    workers.machine_type = options.machine_type
    workers.autoscaling_algorithm = "NONE"

    pipeline = beam.Pipeline(options=pipeline_options)
    parsed = (
        pipeline
        | "ReadTransactionEvents" >> beam.io.ReadFromPubSub(subscription=options.subscription)
        | "ParseValidateAndRoute"
        >> beam.ParDo(ParseAndRoute(options.run_id)).with_outputs(
            "invalid", "observation", main="valid"
        )
    )

    valid_result = parsed.valid | "WriteValidEvents" >> beam.io.WriteToBigQuery(
        table_spec(options.project, options.dataset, "streaming_transaction_events"),
        schema=VALID_EVENT_SCHEMA,
        create_disposition=BigQueryDisposition.CREATE_NEVER,
        write_disposition=BigQueryDisposition.WRITE_APPEND,
        method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
        insert_retry_strategy=RetryStrategy.RETRY_NEVER,
        extended_error_info=True,
    )
    write_failures = (
        valid_result.failed_rows_with_errors
        | "RouteBigQueryWriteFailures"
        >> beam.ParDo(RouteWriteFailure(options.run_id)).with_outputs(
            "observation", main="quarantine"
        )
    )
    (parsed.invalid, write_failures.quarantine) | "MergeQuarantineRows" >> beam.Flatten() | "WriteQuarantine" >> beam.io.WriteToBigQuery(
        table_spec(
            options.project,
            options.dataset,
            "streaming_transaction_events_quarantine",
        ),
        schema=QUARANTINE_SCHEMA,
        create_disposition=BigQueryDisposition.CREATE_NEVER,
        write_disposition=BigQueryDisposition.WRITE_APPEND,
        method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
    )
    (parsed.observation, write_failures.observation) | "MergeObservations" >> beam.Flatten() | "WriteObservations" >> beam.io.WriteToBigQuery(
        table_spec(options.project, options.dataset, "streaming_pipeline_observations"),
        schema=OBSERVATION_SCHEMA,
        create_disposition=BigQueryDisposition.CREATE_NEVER,
        write_disposition=BigQueryDisposition.WRITE_APPEND,
        method=beam.io.WriteToBigQuery.Method.STREAMING_INSERTS,
    )
    return pipeline


def main() -> None:
    logging.getLogger().setLevel(logging.INFO)
    options, beam_args = parse_arguments()
    try:
        pipeline = build_pipeline(options, beam_args)
    except (ValueError, RuntimeError) as exc:
        raise SystemExit(f"ERROR: {exc}") from exc
    print(
        "WARNING: submitting an unbounded Dataflow job. The demo launcher must stop "
        "it within GCP_STREAMING_MAX_DEMO_MINUTES."
    )
    result = pipeline.run()
    job_id = getattr(result, "job_id", lambda: "unknown")()
    print(f"DATAFLOW JOB SUBMITTED job_name={options.job_name} job_id={job_id}")


if __name__ == "__main__":
    main()
