"""Tests for shopstream/1_api_sources/utils.py"""

import importlib.util
import logging
import os
from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import NotFound, GoogleAPIError

# Load utils explicitly from its file path to avoid sys.path conflicts
# with 2_pubsub-2-gcs/utils.py (both are named 'utils').
_utils_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "utils.py")
_spec = importlib.util.spec_from_file_location("api_sources.utils", _utils_path)
_utils = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_utils)

setup_log_execution = _utils.setup_log_execution
get_or_create_pubsub_topic = _utils.get_or_create_pubsub_topic


# ---------------------------------------------------------------------------
# setup_log_execution
# ---------------------------------------------------------------------------


def test_setup_log_execution_sets_correct_level():
    log_levels = {"dev": logging.DEBUG, "stg": logging.INFO, "prd": logging.WARNING}
    setup_log_execution("dev", log_levels)
    assert logging.getLogger().level == logging.DEBUG


def test_setup_log_execution_defaults_to_warning_for_unknown_env():
    log_levels = {"dev": logging.DEBUG}
    setup_log_execution("unknown", log_levels)
    assert logging.getLogger().level == logging.WARNING


# ---------------------------------------------------------------------------
# get_or_create_pubsub_topic — error handling
# ---------------------------------------------------------------------------


def test_get_or_create_pubsub_topic_exists():
    """When topic exists, create_topic should NOT be called."""
    publisher = MagicMock()
    get_or_create_pubsub_topic(publisher, "projects/proj/topics/my-topic")
    publisher.get_topic.assert_called_once()
    publisher.create_topic.assert_not_called()


def test_get_or_create_pubsub_topic_creates_when_not_found():
    """When topic is NotFound, create_topic should be called."""
    publisher = MagicMock()
    publisher.get_topic.side_effect = NotFound("topic not found")
    get_or_create_pubsub_topic(publisher, "projects/proj/topics/new-topic")
    publisher.create_topic.assert_called_once_with(
        request={"name": "projects/proj/topics/new-topic"}
    )


def test_get_or_create_pubsub_topic_propagates_other_errors():
    """Non-NotFound errors (e.g. network) should propagate, not trigger create."""
    publisher = MagicMock()
    publisher.get_topic.side_effect = GoogleAPIError("connection error")
    with pytest.raises(GoogleAPIError):
        get_or_create_pubsub_topic(publisher, "projects/proj/topics/my-topic")
    publisher.create_topic.assert_not_called()
