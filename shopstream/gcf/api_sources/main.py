"""
This cloud function is responsible for simulating the data ingestion process.

GCP Services used:
- Google Cloud Run Functions
- Google Cloud Pub/Sub

Workflow:
- Google Cloud Run Function -> HTTP Request(Local or Airflow Trigger) -> Pub/Sub Topic

"""

import logging

import functions_framework
from flask import jsonify
from google.cloud import pubsub_v1
from main_helper import format_topic_map, send_data_to_pubsub
from utils import read_local_file, setup_log_execution

LOG_LEVELS = {"dev": logging.DEBUG, "stg": logging.INFO, "prd": logging.WARNING}


@functions_framework.http
def main(request):
    """
    HTTP Cloud Function that simulates data ingestion by reading local data files and publishing them to Pub/Sub topics.

    Args:
        request (flask.Request): The HTTP request object containing a JSON body with the following fields:
            - log_env (str): Logging environment ('dev', 'stg', 'prd').
            - source_bucket_name (str): Name of the source bucket (not used in local simulation).
            - pubsub_topics (dict): Mapping of topic names to Pub/Sub topic paths.

    Returns:
        flask.Response: JSON response with published message IDs and HTTP status code 200 on success.

    Workflow:
        1. Reads configuration from the request JSON.
        2. Sets up logging based on the environment.
        3. Loads local data files (e.g., clickstream_events.json, product_catalog.json, customer_support.json).
        4. Converts JSON arrays to NDJSON if needed.
        5. Publishes data to the specified Pub/Sub topics.
        6. Returns the Pub/Sub message IDs in the response.

    """

    request_json = request.get_json(silent=True)

    log_env = request_json.get("log_env", "dev")
    source_bucket_name = request_json.get("source_bucket_name", "your-bucket-name")
    pubsub_topics = request_json.get("pubsub_topics", {})

    assert log_env, "log_env is required"
    assert source_bucket_name, "source_bucket_name is required"
    assert pubsub_topics, "pubsub_topics is required"

    setup_log_execution(log_env, LOG_LEVELS)

    try:
        # TODO: Use faker to generate data instead of reading local files
        clickstream_data = read_local_file("clickstream_events.json")
        customer_support_data = read_local_file("customer_support.json")
        product_catalog_data = read_local_file("product_catalog.json")
        sales_transactions_data = read_local_file("transactions.csv")
        logging.info(f"Loaded source data files for environment: {log_env}")

        data_topic_map: list = format_topic_map(
            clickstream_data,
            customer_support_data,
            product_catalog_data,
            sales_transactions_data,
            pubsub_topics,
        )
        logging.info(f"Data topic map: {data_topic_map}")

        publisher = pubsub_v1.PublisherClient()
        logging.info(f"Publishing to PubSub topics: {pubsub_topics}")

        pubsub_message_ids = send_data_to_pubsub(data_topic_map, publisher)
        logging.info("All messages were published successfully.")

        return jsonify({"message_ids": pubsub_message_ids}), 200

    except Exception as e:
        logging.error(f"Failed to publish messages: {e}")
        raise e
