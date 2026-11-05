import importlib.util
import csv
import io
import json
import os
import sys
from unittest.mock import MagicMock

from flask import Flask


PACKAGE_DIR = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, PACKAGE_DIR)

from generator.models import GenerationRequest, SourceName  # noqa: E402
from generator.service import SyntheticDataGenerator  # noqa: E402


def _config(**overrides):
    values = {
        "profile": "small",
        "seed": 42,
        "pubsub_topics": {
            source.value: f"projects/test/topics/{source.value}"
            for source in SourceName
        },
    }
    values.update(overrides)
    return GenerationRequest.model_validate(values)


def test_generator_is_deterministic_and_relationship_aware():
    first = SyntheticDataGenerator(_config()).generate()
    second = SyntheticDataGenerator(_config()).generate()

    assert first == second
    product_ids = {product.product_id for product in first[SourceName.product_catalog]}
    transaction_users = {
        transaction.user_id for transaction in first[SourceName.sales_transactions]
    }
    assert all(
        item.product_id in product_ids
        for transaction in first[SourceName.sales_transactions]
        for item in transaction.items
    )
    assert all(
        event.user_id in transaction_users for event in first[SourceName.clickstream]
    )
    assert all(
        ticket.user_id in transaction_users
        for ticket in first[SourceName.customer_support]
    )


def test_source_filter_controls_publishing_not_supporting_generation():
    config = _config(sources=["clickstream"], volumes={"clickstream": 7})
    records = SyntheticDataGenerator(config).generate()

    assert config.selected_sources == [SourceName.clickstream]
    assert len(records[SourceName.clickstream]) == 7
    assert records[SourceName.product_catalog]
    assert records[SourceName.sales_transactions]


def test_profile_volumes_can_be_overridden():
    config = _config(volumes={"product_catalog": 3, "sales_transactions": 4})
    records = SyntheticDataGenerator(config).generate()

    assert len(records[SourceName.product_catalog]) == 3
    assert len(records[SourceName.sales_transactions]) == 4


def test_request_rejects_unbounded_dataset():
    try:
        _config(volumes={source.value: 10_000 for source in SourceName})
    except ValueError as error:
        assert "must not exceed 10000 records" in str(error)
    else:
        raise AssertionError("oversized generation request was accepted")


def _load_main():
    utils_spec = importlib.util.spec_from_file_location(
        "utils", os.path.join(PACKAGE_DIR, "utils.py")
    )
    utils_module = importlib.util.module_from_spec(utils_spec)
    sys.modules["utils"] = utils_module
    utils_spec.loader.exec_module(utils_module)
    spec = importlib.util.spec_from_file_location(
        "api_sources.main", os.path.join(PACKAGE_DIR, "main.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_main_publishes_all_sources_by_default(monkeypatch):
    main_module = _load_main()
    publisher = MagicMock()
    publisher.publish.side_effect = lambda *args, **kwargs: MagicMock(
        result=lambda: f"message-{publisher.publish.call_count}"
    )
    monkeypatch.setattr(main_module.pubsub_v1, "PublisherClient", lambda: publisher)
    monkeypatch.setattr(main_module, "get_or_create_pubsub_topic", MagicMock())
    request = MagicMock()
    request.get_json.return_value = _config().model_dump(mode="json")

    with Flask(__name__).app_context():
        response, status = main_module.main.__wrapped__(request)

    payload = json.loads(response.get_data(as_text=True))
    assert status == 200
    assert set(payload["published"]) == {source.value for source in SourceName}
    assert payload["message_count"] == 135


def test_main_publishes_only_selected_source(monkeypatch):
    main_module = _load_main()
    publisher = MagicMock()
    publisher.publish.return_value.result.return_value = "message-1"
    monkeypatch.setattr(main_module.pubsub_v1, "PublisherClient", lambda: publisher)
    monkeypatch.setattr(main_module, "get_or_create_pubsub_topic", MagicMock())
    request = MagicMock()
    request.get_json.return_value = _config(
        sources=["product_catalog"], volumes={"product_catalog": 2}
    ).model_dump(mode="json")

    with Flask(__name__).app_context():
        response, status = main_module.main.__wrapped__(request)

    assert status == 200
    assert json.loads(response.get_data(as_text=True))["published"] == {
        "product_catalog": 2
    }
    assert publisher.publish.call_count == 2


def test_sales_messages_include_a_csv_header(monkeypatch):
    main_module = _load_main()
    publisher = MagicMock()
    publisher.publish.return_value.result.return_value = "message-1"
    monkeypatch.setattr(main_module.pubsub_v1, "PublisherClient", lambda: publisher)
    monkeypatch.setattr(main_module, "get_or_create_pubsub_topic", MagicMock())
    request = MagicMock()
    request.get_json.return_value = _config(
        sources=["sales_transactions"], volumes={"sales_transactions": 1}
    ).model_dump(mode="json")

    with Flask(__name__).app_context():
        _, status = main_module.main.__wrapped__(request)

    payload = publisher.publish.call_args.args[1].decode("utf-8")
    rows = list(csv.DictReader(io.StringIO(payload)))
    assert status == 200
    assert len(rows) == 1
    assert rows[0]["transaction_id"].startswith("txn_")
    assert json.loads(rows[0]["items"])


def test_main_rejects_empty_source_selection():
    main_module = _load_main()
    request = MagicMock()
    request.get_json.return_value = {
        "sources": [],
        "pubsub_topics": {"clickstream": "projects/test/topics/clickstream"},
    }

    with Flask(__name__).app_context():
        response, status = main_module.main.__wrapped__(request)

    assert status == 400
    assert json.loads(response.get_data(as_text=True))["error"] == "invalid_request"
