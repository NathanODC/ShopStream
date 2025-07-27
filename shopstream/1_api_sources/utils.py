import logging
import os
import json


def setup_log_execution(log_env, LOG_LEVELS):
    log_level = LOG_LEVELS.get(log_env, logging.WARNING)
    logging.getLogger().setLevel(log_level)


def get_or_create_pubsub_topic(publisher, topic_path):
    try:
        publisher.get_topic(request={"topic": topic_path})
        print(f"Topic {topic_path} already exists.")
    except Exception:
        publisher.create_topic(request={"name": topic_path})
        print(f"Topic {topic_path} created.")


def read_local_file(path):
    with open(os.path.join("source-data", path), "rb") as f:
        return f.read()


def convert_json_array_to_ndjson(json_array_string) -> bytes:
    # Accept bytes or str, always return bytes
    try:
        if isinstance(json_array_string, bytes):
            json_array_string = json_array_string.decode("utf-8")
        data = json.loads(json_array_string)
        iterator_of_json_strings = map(json.dumps, data)
        ndjson_string = "\n".join(iterator_of_json_strings)
        return ndjson_string.encode("utf-8")
    except json.JSONDecodeError as e:
        print(f"Error: Input string is not a valid JSON array. Details: {e}")
        raise
    except TypeError as e:
        print(f"Error: Input must be a string or bytes. Details: {e}")
        raise
