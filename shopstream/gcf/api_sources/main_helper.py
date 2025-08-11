"""
Helper functions for ShopStream GCF API sources main Cloud Function.

This module provides:
- Data mapping utilities to associate source data with Pub/Sub topics and formats.
- Utilities to publish data to Pub/Sub, including topic creation and NDJSON conversion.

Dependencies:
- google-cloud-pubsub
- logging
- utils (local module)
"""

import logging

from google.cloud import pubsub_v1
from utils import convert_json_array_to_ndjson, get_or_create_pubsub_topic


def format_topic_map(
    clickstream_data: str,
    customer_support_data: str,
    product_catalog_data: str,
    sales_transactions_data: str,
    pubsub_topics: dict,
) -> list:  # TODO: move to a yaml file / env variable / pass as parameter
    """
    Create a mapping between source data and their corresponding Pub/Sub topics and formats.

    Args:
        clickstream_data (str): Data for clickstream events.
        customer_support_data (str): Data for customer support events.
        product_catalog_data (str): Data for product catalog.
        sales_transactions_data (str): Data for sales transactions.
        pubsub_topics (dict): Dictionary mapping logical topic names to Pub/Sub topic paths.

    Returns:
        list: List of dictionaries, each containing data, topic path, and file extension.
    """
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
    return data_topic_map


def send_data_to_pubsub(
    data_topic_map: list, publisher: pubsub_v1.PublisherClient
) -> list:
    """
    Publish mapped data to their respective Pub/Sub topics, creating topics if needed and converting JSON to NDJSON.

    Args:
        data_topic_map (list): List of dicts with data, topic, and extension.
        publisher (pubsub_v1.PublisherClient): Pub/Sub publisher client.

    Returns:
        list: List of published Pub/Sub message IDs.
    """
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

    return pubsub_message_ids
