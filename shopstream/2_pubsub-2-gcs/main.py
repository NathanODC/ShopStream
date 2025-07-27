import os
import logging
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from google.cloud import storage

os.environ["PUBSUB_EMULATOR_HOST"] = "localhost:8134"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = (
    "/home/nathanodc/Projects/Personal/ShopStream/shopstream-proj-b2c90a6ef5a1.json"
)

logging.basicConfig(level=logging.INFO)


TOPIC_BUCKET_MAP = {
    "projects/local-pubsub-instance/topics/clickstream-topic": [
        "shopstream-bronze-events",
        "clickstream",
        "json",
    ],
    "projects/local-pubsub-instance/topics/customer-support-topic": [
        "shopstream-bronze-support",
        "customer-support",
        "json",
    ],
    "projects/local-pubsub-instance/topics/product-catalog": [
        "shopstream-bronze-products",
        "product-catalog",
        "json",
    ],
    "projects/local-pubsub-instance/topics/sales-transactions": [
        "shopstream-bronze-sales",
        "sales-transactions",
        "csv",
    ],
}

GCS_PREFIX = "pubsub-data"
TIMEOUT = 100.0


def upload_to_gcs(bucket_name, blob_name, data):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_string(data)
    logging.info(f"Uploaded to gs://{bucket_name}/{blob_name}")


def callback_factory(topic, data_params):
    def callback(message):
        bucket_name = data_params[0]
        file_name = data_params[1]
        file_extension = data_params[2]

        logging.info(f"Received message on {topic}: {message.message_id}")
        blob_name = f"{GCS_PREFIX}/{topic.split('/')[-1]}/dt={message.publish_time.strftime('%Y-%m-%d')}/{file_name}_{message.publish_time.strftime('%Y-%m-%d_%H-%M-%S')}.{file_extension}"
        upload_to_gcs(bucket_name, blob_name, message.data)
        message.ack()

    return callback


def main():
    subscriber = pubsub_v1.SubscriberClient()
    streaming_pull_futures = []

    for topic, data_params in TOPIC_BUCKET_MAP.items():
        subscription_path = topic.replace("topics", "subscriptions") + "-sub"
        try:
            subscriber.get_subscription(request={"subscription": subscription_path})
            logging.info(f"Subscription {subscription_path} already exists.")
        except Exception:
            subscriber.create_subscription(name=subscription_path, topic=topic)
            logging.info(f"Created subscription {subscription_path} for topic {topic}.")

        callback = callback_factory(topic, data_params)
        future = subscriber.subscribe(subscription_path, callback=callback)
        streaming_pull_futures.append(future)
        logging.info(f"Listening for messages on {subscription_path}...")

    try:
        for future in streaming_pull_futures:
            future.result(timeout=TIMEOUT)
    except TimeoutError:
        logging.info("Stopped listening after timeout.")
    except KeyboardInterrupt:
        logging.info("Interrupted by user.")
    finally:
        for future in streaming_pull_futures:
            future.cancel()


if __name__ == "__main__":
    main()
