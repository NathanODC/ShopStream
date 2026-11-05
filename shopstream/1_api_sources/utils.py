import logging

from google.api_core.exceptions import NotFound


def setup_log_execution(log_env, LOG_LEVELS):
    log_level = LOG_LEVELS.get(log_env, logging.WARNING)
    logging.getLogger().setLevel(log_level)
    logging.debug(
        f"Logging initialized for environment '{log_env}' with level {logging.getLevelName(log_level)}."
    )


def get_or_create_pubsub_topic(publisher, topic_path):
    try:
        publisher.get_topic(request={"topic": topic_path})
        logging.info(f"Topic {topic_path} already exists.")
    except NotFound:
        publisher.create_topic(request={"name": topic_path})
        logging.info(f"Topic {topic_path} created.")
