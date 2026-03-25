import functions_framework
import json
import logging
import os
from concurrent.futures import TimeoutError
from google.api_core.exceptions import NotFound
from google.cloud import pubsub_v1

from main_helper import callback_factory
from utils import setup_log_execution


LOG_LEVELS = {"dev": logging.DEBUG, "stg": logging.INFO, "prd": logging.WARNING}


def load_topic_bucket_map():
    config_file = os.environ.get("CONFIG_PATH", "config.json")
    config_path = os.path.join(os.path.dirname(__file__), config_file)
    with open(config_path) as f:
        return json.load(f)["topic_bucket_map"]


TOPIC_BUCKET_MAP = load_topic_bucket_map()


@functions_framework.http
def main(request):
    request_json = request.get_json(silent=True)

    log_env = request_json.get("log_env", "dev")
    gcs_prefix = request_json.get("gcs_prefix", "pubsub-data")
    timeout = request_json.get("timeout", 100.0)

    assert log_env, "log_env is required"
    assert gcs_prefix, "gcs_prefix is required"
    assert timeout, "timeout is required"

    setup_log_execution(log_env, LOG_LEVELS)

    subscriber = pubsub_v1.SubscriberClient()
    streaming_pull_futures = []

    logging.info("Starting Pub/Sub to GCS pipeline...")
    for topic, data_params in TOPIC_BUCKET_MAP.items():
        subscription_path = topic.replace("topics", "subscriptions") + "-sub"
        try:
            subscriber.get_subscription(request={"subscription": subscription_path})
            logging.info(f"Subscription {subscription_path} already exists.")
        except NotFound:
            subscriber.create_subscription(name=subscription_path, topic=topic)
            logging.info(f"Created subscription {subscription_path} for topic {topic}.")

        callback = callback_factory(topic, data_params, gcs_prefix)
        future = subscriber.subscribe(subscription_path, callback=callback)
        streaming_pull_futures.append(future)
        logging.info(f"Listening for messages on {subscription_path}...")

    try:
        for future in streaming_pull_futures:
            logging.debug(f"Waiting for messages with timeout={timeout}s...")
            future.result(timeout=timeout)
    except TimeoutError:
        logging.warning("Stopped listening after timeout.")
    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
    except Exception as e:
        logging.error(f"Unexpected error in main loop: {e}")
    finally:
        for future in streaming_pull_futures:
            future.cancel()
        logging.info("All streaming pulls cancelled. Exiting.")
