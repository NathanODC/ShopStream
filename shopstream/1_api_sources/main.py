from flask import jsonify
import functions_framework
import logging
from google.cloud import pubsub_v1
import os

from .utils import (
    setup_log_execution,
    get_or_create_pubsub_topic,
    read_local_file,
    convert_json_array_to_ndjson,
)

LOG_LEVELS = {"dev": logging.DEBUG, "stg": logging.INFO, "prd": logging.WARNING}

os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8134"


@functions_framework.http
def hello_http(request):
    """
    HTTP Cloud Function that generates the data simulating the sources.
    """

    request_json = request.get_json(silent=True)

    log_env = request_json.get("log_env", "dev")
    source_bucket_name = request_json.get("source_bucket_name", "your-bucket-name")
    pubsub_topics = request_json.get("pubsub_topics", {})

    assert log_env, "log_env is required"
    assert source_bucket_name, "source_bucket_name is required"
    assert pubsub_topics, "pubsub_topics is required"

    setup_log_execution(log_env, LOG_LEVELS)

    # Environment-specific logging
    if log_env == "dev":
        logging.debug("[DEV] Starting data ingestion simulation in development mode.")
    elif log_env == "stg":
        logging.info("[STAGING] Starting data ingestion simulation in staging mode.")
    elif log_env == "prd":
        logging.warning("[PRODUCTION] Starting data ingestion simulation in production mode.")
    else:
        logging.info(f"[UNKNOWN ENV] Starting data ingestion simulation in environment: {log_env}")

    try:
        clickstream_data = read_local_file("clickstream_events.json")
        customer_support_data = read_local_file("customer_support.json")
        product_catalog_data = read_local_file("product_catalog.json")
        sales_transactions_data = read_local_file("transactions.csv")

        logging.info(f"Loaded source data files for environment: {log_env}")

        data_topic_map = [
            {
                "data": clickstream_data,
                "topic": pubsub_topics.get("clickstream"),
                "extension": "json",
            },
            {
                "data": customer_support_data,
                "topic": pubsub_topics.get("customer_support"),
                "extension": "json",
            },
            {
                "data": product_catalog_data,
                "topic": pubsub_topics.get("product_catalog"),
                "extension": "json",
            },
            {
                "data": sales_transactions_data,
                "topic": pubsub_topics.get("sales_transactions"),
                "extension": "csv",
            },
        ]

        publisher = pubsub_v1.PublisherClient()

        logging.info(f"Publishing to PubSub topics: {pubsub_topics}")

        for item in data_topic_map:
            get_or_create_pubsub_topic(publisher, item["topic"])

        futures = []
        for item in data_topic_map:
            logging.info(item)
            if item["extension"] == "json":
                item["data"] = convert_json_array_to_ndjson(item["data"])
                logging.info(item["data"])

            futures.append(publisher.publish(item["topic"], item["data"]))

        pubsub_message_ids = [f.result() for f in futures]
        logging.info(f"Published messages with IDs: {pubsub_message_ids}")

        return jsonify({"message_ids": pubsub_message_ids}), 200

    except Exception as e:
        logging.error(f"Failed to publish messages: {e}")
        raise e
