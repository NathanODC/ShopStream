import logging
import os

from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError


def setup_log_execution(log_env, LOG_LEVELS):
    log_level = LOG_LEVELS.get(log_env, logging.WARNING)
    logging.getLogger().setLevel(log_level)
    logging.debug(
        f"Logging initialized for environment '{log_env}' with level {logging.getLevelName(log_level)}."
    )


def _make_storage_client():
    emulator_host = os.environ.get("STORAGE_EMULATOR_HOST")
    if emulator_host:
        from google.auth.credentials import AnonymousCredentials
        return storage.Client(
            credentials=AnonymousCredentials(),
            project="local",
            client_options={"api_endpoint": emulator_host},
        )
    return storage.Client()


def upload_to_gcs(bucket_name, blob_name, data):
    try:
        storage_client = _make_storage_client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(data)
        logging.info(f"Uploaded to gs://{bucket_name}/{blob_name}")
    except GoogleCloudError as e:
        logging.error(f"Failed to upload to gs://{bucket_name}/{blob_name}: {e}")
        raise
