import functions_framework
import logging
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from flask import jsonify

from main_helper import callback_factory
from utils import setup_log_execution


LOG_LEVELS = {"dev": logging.DEBUG, "stg": logging.INFO, "prd": logging.WARNING}


TOPIC_BUCKET_MAP = {  # TODO: move to a yaml file / env variable / pass as parameter
    "projects/shopstream-proj/topics/clickstream-topic": [
        "shopstream-bronze-events",
        "clickstream",
        "json",
    ],
    "projects/shopstream-proj/topics/customer-support-topic": [
        "shopstream-bronze-support",
        "customer-support",
        "json",
    ],
    "projects/shopstream-proj/topics/product-catalog": [
        "shopstream-bronze-products",
        "product-catalog",
        "json",
    ],
    "projects/shopstream-proj/topics/sales-transactions": [
        "shopstream-bronze-sales",
        "sales-transactions",
        "csv",
    ],
}


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
        except Exception:
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
        return jsonify({"message": "Stopped listening after timeout."}), 200
    except KeyboardInterrupt:
        logging.warning("Interrupted by user.")
        return jsonify({"message": "Interrupted by user."}), 200
    except Exception as e:
        logging.error(f"Unexpected error in main loop: {e}")
        return jsonify({"message": "Unexpected error in main loop: {e}"}), 500
    finally:
        for future in streaming_pull_futures:
            future.cancel()
        logging.info("All streaming pulls cancelled. Exiting.")
