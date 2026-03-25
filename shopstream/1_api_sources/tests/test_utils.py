"""Tests for shopstream/1_api_sources/utils.py"""
import importlib.util
import json
import logging
import os
import pytest
from unittest.mock import MagicMock

from google.api_core.exceptions import NotFound, GoogleAPIError

# Load utils explicitly from its file path to avoid sys.path conflicts
# with 2_pubsub-2-gcs/utils.py (both are named 'utils').
_utils_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "utils.py")
_spec = importlib.util.spec_from_file_location("api_sources.utils", _utils_path)
_utils = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_utils)

setup_log_execution = _utils.setup_log_execution
get_or_create_pubsub_topic = _utils.get_or_create_pubsub_topic
read_local_file = _utils.read_local_file
convert_json_array_to_ndjson = _utils.convert_json_array_to_ndjson


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
# convert_json_array_to_ndjson
# ---------------------------------------------------------------------------

def test_convert_json_array_to_ndjson_basic():
    data = json.dumps([{"a": 1}, {"b": 2}]).encode("utf-8")
    result = convert_json_array_to_ndjson(data)
    lines = result.decode("utf-8").strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"a": 1}
    assert json.loads(lines[1]) == {"b": 2}


def test_convert_json_array_to_ndjson_accepts_str():
    data = json.dumps([{"x": 10}])
    result = convert_json_array_to_ndjson(data)
    assert isinstance(result, bytes)
    assert json.loads(result.decode("utf-8")) == {"x": 10}


def test_convert_json_array_to_ndjson_raises_on_invalid_json():
    with pytest.raises(json.JSONDecodeError):
        convert_json_array_to_ndjson(b"not valid json")


def test_convert_json_array_to_ndjson_raises_on_wrong_type():
    with pytest.raises(TypeError):
        convert_json_array_to_ndjson(12345)


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


# ---------------------------------------------------------------------------
# read_local_file
# ---------------------------------------------------------------------------

def test_read_local_file_reads_content(tmp_path, monkeypatch):
    source_dir = tmp_path / "source-data"
    source_dir.mkdir()
    (source_dir / "test.json").write_bytes(b'[{"id": 1}]')
    monkeypatch.chdir(tmp_path)
    result = read_local_file("test.json")
    assert result == b'[{"id": 1}]'


def test_read_local_file_raises_on_missing_file(tmp_path, monkeypatch):
    (tmp_path / "source-data").mkdir()
    monkeypatch.chdir(tmp_path)
    with pytest.raises(FileNotFoundError):
        read_local_file("nonexistent.json")
