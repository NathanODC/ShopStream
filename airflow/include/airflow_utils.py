import logging

import requests

logger = logging.getLogger("airflow.task")


def call_cloud_function(url: str, args: dict, timeout: int = 300) -> dict:
    logger.info(f"Calling GCF at {url} with args: {args}")
    response = requests.post(url, json=args, timeout=timeout)
    response.raise_for_status()
    result = response.json()
    logger.info(f"GCF response: {result}")
    return result
