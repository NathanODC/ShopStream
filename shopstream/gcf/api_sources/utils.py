"""
Utility functions for ShopStream GCF API sources.

This module provides helper functions for:
- Setting up logging based on environment variables.
- Managing Google Cloud Pub/Sub topics (creation and existence checks).
- Reading local files from a specific source-data directory.
- Converting JSON arrays to newline-delimited JSON (NDJSON) format.

Dependencies:
- google-cloud-pubsub
- logging
- os
- json

"""

import json
import logging
import os

from google.cloud import pubsub_v1


def setup_log_execution(log_env: str, LOG_LEVELS: dict) -> None:
    """
    Configure the logging level for the application based on the environment.

    Args:
        log_env (str): The environment name (e.g., 'dev', 'stg', 'prd').
        LOG_LEVELS (dict): A mapping from environment names to logging levels.
    """

    log_level = LOG_LEVELS.get(log_env, logging.WARNING)
    logging.getLogger().setLevel(log_level)
    logging.debug(
        f"Logging initialized for environment '{log_env}' with level {logging.getLevelName(log_level)}."
    )

    # Environment-specific logging
    if log_env == "dev":
        logging.debug("[DEV] Starting data ingestion simulation in development mode.")
    elif log_env == "stg":
        logging.info("[STAGING] Starting data ingestion simulation in staging mode.")
    elif log_env == "prd":
        logging.warning(
            "[PRODUCTION] Starting data ingestion simulation in production mode."
        )
    else:
        logging.info(
            f"[UNKNOWN ENV] Starting data ingestion simulation in environment: {log_env}"
        )


def get_or_create_pubsub_topic(
    publisher: pubsub_v1.PublisherClient, topic_path: str
) -> None:
    """
    Ensure a Pub/Sub topic exists; create it if it does not.

    Args:
        publisher (pubsub_v1.PublisherClient): The Pub/Sub publisher client.
        topic_path (str): The full path of the Pub/Sub topic.
    """
    try:
        publisher.get_topic(request={"topic": topic_path})
        logging.info(f"Topic {topic_path} already exists.")
    except Exception:
        publisher.create_topic(request={"name": topic_path})
        logging.info(f"Topic {topic_path} created.")


def read_local_file(path: str) -> bytes:
    """
    Read a file from the /source-data directory as bytes.

    Args:
        path (str): Relative path to the file inside /source-data.

    Returns:
        bytes: The contents of the file.

    Raises:
        Exception: If the file cannot be read.
    """
    try:
        file_path = os.path.join("/", "source-data", path)
    except Exception as e:
        logging.error(f"Failed to read file {file_path}: {e}")
        raise
    try:
        with open(file_path, "rb") as f:
            data = f.read()
        logging.debug(f"Successfully read file: {file_path}")
        return data
    except Exception as e:
        logging.error(f"Failed to read file {file_path}: {e}")
        raise


def convert_json_array_to_ndjson(json_array_string: str) -> bytes:
    """
    Convert a JSON array (as string or bytes) to newline-delimited JSON (NDJSON) bytes.

    Args:
        json_array_string (str or bytes): JSON array input as a string or bytes.

    Returns:
        bytes: NDJSON formatted data as bytes.

    Raises:
        json.JSONDecodeError: If the input is not valid JSON.
        TypeError: If the input is not a string or bytes.
    """
    # Accept bytes or str, always return bytes
    try:
        if isinstance(json_array_string, bytes):
            json_array_string = json_array_string.decode("utf-8")
        data = json.loads(json_array_string)
        iterator_of_json_strings = map(json.dumps, data)
        ndjson_string = "\n".join(iterator_of_json_strings)
        logging.debug("Successfully converted JSON array to NDJSON format.")
        return ndjson_string.encode("utf-8")
    except json.JSONDecodeError as e:
        logging.error(f"Error: Input string is not a valid JSON array. Details: {e}")
        raise
    except TypeError as e:
        logging.error(f"Error: Input must be a string or bytes. Details: {e}")
        raise
