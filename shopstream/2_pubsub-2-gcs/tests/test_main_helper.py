"""Tests for shopstream/2_pubsub-2-gcs/main_helper.py"""
import importlib.util
import os
import sys
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone

from google.cloud.exceptions import GoogleCloudError

_pkg_dir = os.path.dirname(os.path.dirname(__file__))


def _load_module(name, filename):
    spec = importlib.util.spec_from_file_location(name, os.path.join(_pkg_dir, filename))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


# Load utils first so main_helper can find it
_utils_mod = _load_module("pubsub_gcs.utils", "utils.py")
# Inject as bare 'utils' so main_helper's `from utils import upload_to_gcs` resolves
sys.modules["utils"] = _utils_mod

_main_helper = _load_module("pubsub_gcs.main_helper", "main_helper.py")
callback_factory = _main_helper.callback_factory


def _make_message(message_id="msg-1", data=b"payload"):
    msg = MagicMock()
    msg.message_id = message_id
    msg.data = data
    msg.publish_time = datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
    return msg


TOPIC = "projects/shopstream-proj/topics/clickstream-topic"
DATA_PARAMS = ["shopstream-bronze-events", "clickstream", "json"]
GCS_PREFIX = "pubsub-data"


def test_callback_acks_on_success():
    """Message is acked when GCS upload succeeds."""
    with patch.object(_utils_mod, "storage"):
        with patch.object(_utils_mod, "upload_to_gcs") as mock_upload:
            # Patch upload_to_gcs on the module main_helper imported
            _main_helper.upload_to_gcs = mock_upload
            cb = callback_factory(TOPIC, DATA_PARAMS, GCS_PREFIX)
            msg = _make_message()
            cb(msg)
            mock_upload.assert_called_once()
            msg.ack.assert_called_once()
            msg.nack.assert_not_called()


def test_callback_nacks_on_gcs_error():
    """Message is nacked (not silently dropped) when GCS upload fails."""
    mock_upload = MagicMock(side_effect=GoogleCloudError("upload failed"))
    _main_helper.upload_to_gcs = mock_upload
    cb = callback_factory(TOPIC, DATA_PARAMS, GCS_PREFIX)
    msg = _make_message()
    cb(msg)
    msg.nack.assert_called_once()
    msg.ack.assert_not_called()


def test_callback_blob_name_format():
    """Blob name contains gcs_prefix, topic slug, date partition, and timestamp."""
    captured = {}

    def capture_upload(bucket_name, blob_name, data):
        captured["blob_name"] = blob_name

    _main_helper.upload_to_gcs = capture_upload
    cb = callback_factory(TOPIC, DATA_PARAMS, GCS_PREFIX)
    cb(_make_message())

    blob = captured["blob_name"]
    assert blob.startswith("pubsub-data/clickstream-topic/dt=2024-06-15/")
    assert blob.endswith(".json")


def test_callback_uses_correct_bucket():
    """Callback passes the correct bucket name from data_params."""
    captured = {}

    def capture_upload(bucket_name, blob_name, data):
        captured["bucket"] = bucket_name

    _main_helper.upload_to_gcs = capture_upload
    cb = callback_factory(TOPIC, DATA_PARAMS, GCS_PREFIX)
    cb(_make_message())

    assert captured["bucket"] == "shopstream-bronze-events"
