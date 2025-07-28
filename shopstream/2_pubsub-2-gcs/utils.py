from google.cloud import storage
import logging


def setup_log_execution(log_env, LOG_LEVELS):
    log_level = LOG_LEVELS.get(log_env, logging.WARNING)
    logging.getLogger().setLevel(log_level)
    logging.debug(
        f"Logging initialized for environment '{log_env}' with level {logging.getLevelName(log_level)}."
    )


def upload_to_gcs(bucket_name, blob_name, data):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_string(data)
        logging.info(f"Uploaded to gs://{bucket_name}/{blob_name}")
    except Exception as e:
        logging.error(f"Failed to upload to gs://{bucket_name}/{blob_name}: {e}")
        raise