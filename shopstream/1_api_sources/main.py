import json
import logging

import functions_framework
from flask import jsonify
from google.cloud import pubsub_v1
from pydantic import ValidationError

from generator.models import GenerationRequest, SourceName
from generator.service import SyntheticDataGenerator
from utils import get_or_create_pubsub_topic, setup_log_execution


LOG_LEVELS = {"dev": logging.DEBUG, "stg": logging.INFO, "prd": logging.WARNING}


def _serialize(source: SourceName, record) -> bytes:
    data = record.model_dump(mode="json")
    return json.dumps(data, separators=(",", ":")).encode("utf-8")


@functions_framework.http
def main(request):
    """Generate a coherent synthetic retail dataset and publish it to Pub/Sub."""
    request_json = request.get_json(silent=True) or {}
    setup_log_execution(request_json.get("log_env", "dev"), LOG_LEVELS)

    try:
        config = GenerationRequest.model_validate(request_json)
    except ValidationError as error:
        return (
            jsonify(
                {
                    "error": "invalid_request",
                    "details": error.errors(include_context=False, include_url=False),
                }
            ),
            400,
        )

    missing_topics = [
        source.value
        for source in config.selected_sources
        if not config.pubsub_topics.get(source.value)
    ]
    if missing_topics:
        return (
            jsonify({"error": "missing_pubsub_topics", "sources": missing_topics}),
            400,
        )

    records = SyntheticDataGenerator(config).generate()
    publisher = pubsub_v1.PublisherClient()
    for source in config.selected_sources:
        get_or_create_pubsub_topic(publisher, config.pubsub_topics[source.value])

    futures = []
    published = {}
    for source in config.selected_sources:
        topic = config.pubsub_topics[source.value]
        source_records = records[source]
        published[source.value] = len(source_records)
        for record in source_records:
            event_time = (
                getattr(record, "timestamp", None)
                or getattr(record, "created_at", None)
                or getattr(record, "last_updated", None)
            )
            futures.append(
                publisher.publish(
                    topic,
                    _serialize(source, record),
                    source=source.value,
                    schema_version="1.0",
                    event_time=event_time.isoformat(),
                    seed=str(config.seed),
                )
            )

    message_ids = [future.result() for future in futures]
    logging.info("Published %s synthetic records", len(message_ids))
    return (
        jsonify(
            {
                "profile": config.profile,
                "seed": config.seed,
                "published": published,
                "message_count": len(message_ids),
                "message_ids": message_ids,
            }
        ),
        200,
    )
