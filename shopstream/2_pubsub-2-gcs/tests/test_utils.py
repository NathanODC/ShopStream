"""Tests for shopstream/2_pubsub-2-gcs/utils.py"""
import importlib.util
import os
import sys
import pytest
from unittest.mock import MagicMock, patch

from google.cloud.exceptions import GoogleCloudError

_pkg_dir = os.path.dirname(os.path.dirname(__file__))

# Load upload_to_gcs explicitly to avoid collision with 1_api_sources/utils.py
_spec = importlib.util.spec_from_file_location(
    "pubsub_gcs.utils", os.path.join(_pkg_dir, "utils.py")
)
_utils = importlib.util.module_from_spec(_spec)
sys.modules["pubsub_gcs.utils"] = _utils
_spec.loader.exec_module(_utils)

upload_to_gcs = _utils.upload_to_gcs


def test_upload_to_gcs_success():
    """Successful upload calls upload_from_string."""
    with patch.object(_utils.storage, "Client") as mock_client_cls:
        mock_blob = MagicMock()
        mock_client_cls.return_value.bucket.return_value.blob.return_value = mock_blob
        upload_to_gcs("my-bucket", "path/to/blob.json", b'{"data": 1}')
        mock_blob.upload_from_string.assert_called_once_with(b'{"data": 1}')


def test_upload_to_gcs_raises_google_cloud_error():
    """GoogleCloudError is logged and re-raised."""
    with patch.object(_utils.storage, "Client") as mock_client_cls:
        mock_blob = MagicMock()
        mock_blob.upload_from_string.side_effect = GoogleCloudError("403 Forbidden")
        mock_client_cls.return_value.bucket.return_value.blob.return_value = mock_blob
        with pytest.raises(GoogleCloudError):
            upload_to_gcs("my-bucket", "path/blob.json", b"data")


def test_upload_to_gcs_other_exceptions_propagate():
    """Non-GCP errors also propagate."""
    with patch.object(_utils.storage, "Client") as mock_client_cls:
        mock_client_cls.side_effect = RuntimeError("unexpected")
        with pytest.raises(RuntimeError):
            upload_to_gcs("my-bucket", "path/blob.json", b"data")
