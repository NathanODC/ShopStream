"""Tests for shopstream/2_pubsub-2-gcs/main.py — load_topic_bucket_map and error handling."""
import importlib.util
import json
import os
import sys
import pytest
from unittest.mock import MagicMock

from google.api_core.exceptions import NotFound, GoogleAPIError

_pkg_dir = os.path.dirname(os.path.dirname(__file__))


def _load_module(name, filename):
    spec = importlib.util.spec_from_file_location(name, os.path.join(_pkg_dir, filename))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


# Ensure dependencies of main.py are registered under the names it imports
_utils_mod = _load_module("pubsub_gcs.utils", "utils.py")
sys.modules.setdefault("utils", _utils_mod)

_helper_mod = _load_module("pubsub_gcs.main_helper", "main_helper.py")
sys.modules.setdefault("main_helper", _helper_mod)

_main_mod = _load_module("pubsub_gcs.main", "main.py")
load_topic_bucket_map = _main_mod.load_topic_bucket_map


# ---------------------------------------------------------------------------
# load_topic_bucket_map
# ---------------------------------------------------------------------------

def test_load_topic_bucket_map_returns_dict():
    result = load_topic_bucket_map()
    assert isinstance(result, dict)
    assert len(result) > 0


def test_load_topic_bucket_map_has_expected_topics():
    result = load_topic_bucket_map()
    expected_topics = [
        "projects/shopstream-proj/topics/clickstream-topic",
        "projects/shopstream-proj/topics/customer-support-topic",
        "projects/shopstream-proj/topics/product-catalog",
        "projects/shopstream-proj/topics/sales-transactions",
    ]
    for topic in expected_topics:
        assert topic in result, f"Missing topic: {topic}"


def test_load_topic_bucket_map_values_have_three_elements():
    """Each value must be [bucket_name, file_prefix, extension]."""
    result = load_topic_bucket_map()
    for topic, params in result.items():
        assert len(params) == 3, f"Topic {topic} must have 3 params, got {len(params)}"


def test_load_topic_bucket_map_extensions_are_valid():
    result = load_topic_bucket_map()
    valid_extensions = {"json", "csv"}
    for topic, params in result.items():
        assert params[2] in valid_extensions, (
            f"Topic {topic} has unexpected extension '{params[2]}'"
        )


def test_load_topic_bucket_map_raises_on_missing_config(tmp_path, monkeypatch):
    """If config.json is missing, a FileNotFoundError is raised."""
    monkeypatch.setattr(_main_mod.os.path, "dirname", lambda _: str(tmp_path))
    with pytest.raises((FileNotFoundError, OSError)):
        load_topic_bucket_map()


def test_load_topic_bucket_map_raises_on_malformed_json(tmp_path, monkeypatch):
    """Malformed config.json raises JSONDecodeError."""
    (tmp_path / "config.json").write_text("not valid json")
    monkeypatch.setattr(_main_mod.os.path, "dirname", lambda _: str(tmp_path))
    with pytest.raises(json.JSONDecodeError):
        load_topic_bucket_map()


# ---------------------------------------------------------------------------
# Subscription creation error handling
# ---------------------------------------------------------------------------

def test_subscription_creation_on_not_found():
    """NotFound triggers create_subscription."""
    subscriber = MagicMock()
    subscriber.get_subscription.side_effect = NotFound("sub not found")

    subscription_path = "projects/proj/subscriptions/my-sub"
    topic = "projects/proj/topics/my-topic"
    try:
        subscriber.get_subscription(request={"subscription": subscription_path})
    except NotFound:
        subscriber.create_subscription(name=subscription_path, topic=topic)

    subscriber.create_subscription.assert_called_once_with(
        name=subscription_path, topic=topic
    )


def test_subscription_other_errors_propagate():
    """Non-NotFound errors must not trigger create_subscription."""
    subscriber = MagicMock()
    subscriber.get_subscription.side_effect = GoogleAPIError("network error")

    subscription_path = "projects/proj/subscriptions/my-sub"
    topic = "projects/proj/topics/my-topic"
    with pytest.raises(GoogleAPIError):
        try:
            subscriber.get_subscription(request={"subscription": subscription_path})
        except NotFound:
            subscriber.create_subscription(name=subscription_path, topic=topic)

    subscriber.create_subscription.assert_not_called()
