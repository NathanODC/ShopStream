"""
ShopStream Dataflow streaming pipeline.

Reads messages from a single Pub/Sub topic, transforms them generically,
and writes to a BigQuery table using the schema defined in config.py.

Usage — local (DirectRunner + Pub/Sub emulator):
    export PUBSUB_EMULATOR_HOST=localhost:8085
    python pipeline.py \\
        --runner=DirectRunner \\
        --project=local-project \\
        --topic=projects/local-pubsub-instance/topics/clickstream-topic \\
        --bq_dataset=shopstream_streaming \\
        --bq_table=clickstream_raw \\
        --temp_location=/tmp/beam-temp

Usage — Dataflow (GCP):
    python pipeline.py \\
        --runner=DataflowRunner \\
        --project=shopstream-proj \\
        --region=us-west1 \\
        --topic=projects/shopstream-proj/topics/clickstream-topic \\
        --bq_dataset=shopstream_streaming \\
        --bq_table=clickstream_raw \\
        --temp_location=gs://shopstream-dataflow-temp/temp \\
        --staging_location=gs://shopstream-dataflow-temp/staging

One pipeline instance should be launched per topic.  Use --bq_table to set a
topic-specific destination table; the default is derived from TOPIC_TABLE_MAP
in config.py.
"""

import argparse
import logging

import apache_beam as beam
from apache_beam.io import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
    GoogleCloudOptions,
    SetupOptions,
)

from config import BQ_SCHEMA, TOPIC_TABLE_MAP, DEFAULT_BQ_DATASET
from transforms import ParseMessage, AddMetadata

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------

def _parse_args(argv=None):
    parser = argparse.ArgumentParser(description="ShopStream Dataflow streaming pipeline")

    parser.add_argument(
        "--topic",
        required=True,
        help="Full Pub/Sub topic path, e.g. projects/shopstream-proj/topics/clickstream-topic",
    )
    parser.add_argument(
        "--bq_dataset",
        default=DEFAULT_BQ_DATASET,
        help=f"BigQuery dataset (default: {DEFAULT_BQ_DATASET})",
    )
    parser.add_argument(
        "--bq_table",
        default=None,
        help=(
            "BigQuery table name. Defaults to the value in TOPIC_TABLE_MAP "
            "for the given topic, or 'streaming_raw' if the topic is not mapped."
        ),
    )
    parser.add_argument(
        "--project",
        default=None,
        help="GCP project ID (also set via --project in PipelineOptions)",
    )

    # Split known args so that Beam's own flags (--runner, --region, etc.) pass through.
    known, pipeline_args = parser.parse_known_args(argv)
    return known, pipeline_args


# ---------------------------------------------------------------------------
# Pipeline construction
# ---------------------------------------------------------------------------

def build_pipeline(topic: str, bq_destination: str, pipeline_options: PipelineOptions):
    """
    Constructs and returns the Beam pipeline object.

    Separating construction from execution makes the pipeline testable
    without triggering a runner.
    """
    p = beam.Pipeline(options=pipeline_options)

    (
        p
        | "ReadFromPubSub" >> ReadFromPubSub(
            topic=topic,
            with_attributes=True,   # gives us message_id, publish_time, attributes
        )
        | "ParseMessage"  >> ParseMessage()
        | "AddMetadata"   >> AddMetadata(source_topic=topic)
        | "WriteToBigQuery" >> WriteToBigQuery(
            table=bq_destination,
            schema=BQ_SCHEMA,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            # STREAMING_INSERTS is the default for unbounded sources;
            # FILE_LOADS can be used for higher throughput at the cost of latency.
            method=WriteToBigQuery.Method.STREAMING_INSERTS,
        )
    )

    return p


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run(argv=None):
    known_args, pipeline_args = _parse_args(argv)

    # Resolve BQ table
    bq_table = known_args.bq_table or TOPIC_TABLE_MAP.get(known_args.topic, "streaming_raw")
    bq_destination = f"{known_args.bq_dataset}.{bq_table}"
    if known_args.project:
        bq_destination = f"{known_args.project}:{bq_destination}"

    logger.info(f"Topic       : {known_args.topic}")
    logger.info(f"Destination : {bq_destination}")

    # Build pipeline options from remaining argv so Beam flags are honoured.
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    # Streaming mode is required for Pub/Sub sources.
    pipeline_options.view_as(StandardOptions).streaming = True

    if known_args.project:
        pipeline_options.view_as(GoogleCloudOptions).project = known_args.project

    p = build_pipeline(known_args.topic, bq_destination, pipeline_options)
    result = p.run()

    runner = pipeline_options.view_as(StandardOptions).runner or "DirectRunner"
    if runner.lower() == "directrunner":
        logger.info("DirectRunner: waiting for pipeline to finish (Ctrl-C to stop)…")
        result.wait_until_finish()
    else:
        logger.info(f"Dataflow job submitted. Monitor at: https://console.cloud.google.com/dataflow")


if __name__ == "__main__":
    run()
