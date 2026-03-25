"""
Unit tests for shopstream/3_dataflow/transforms.py.

Uses apache_beam.testing.TestPipeline (DirectRunner) so tests run locally
without any GCP credentials.  Each test exercises a single transform in
isolation via beam.testing.util.assert_that.
"""

import json
import os
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage as RealPubsubMessage
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
import pytest

# Add 3_dataflow/ to sys.path so that:
# (a) `import transforms` resolves to 3_dataflow/transforms.py
# (b) Beam's pickle serialiser can reimport DoFn classes by their module name.
# This must happen before any Beam import that touches DoFn registration.
_pkg_dir = os.path.dirname(os.path.dirname(__file__))
if _pkg_dir not in sys.path:
    sys.path.insert(0, _pkg_dir)

from transforms import ParseMessage, AddMetadata, _ParseMessageFn, _AddMetadataFn  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_pubsub_message(data: bytes, message_id: str = "msg-001", attributes: dict = None):
    """MagicMock-based message for DoFn unit tests (process() called directly)."""
    msg = MagicMock(spec=beam.io.PubsubMessage)
    msg.data = data
    msg.message_id = message_id
    msg.publish_time = datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc)
    msg.attributes = attributes or {}
    return msg


def _make_real_pubsub_message(data: bytes, message_id: str = "msg-001", attributes: dict = None):
    """Real PubsubMessage for pipeline integration tests — must be picklable for Beam."""
    return RealPubsubMessage(
        data=data,
        attributes=attributes or {},
        message_id=message_id,
        publish_time=datetime(2024, 6, 15, 10, 30, 0, tzinfo=timezone.utc),
    )


# ---------------------------------------------------------------------------
# _ParseMessageFn — unit tests (DoFn.process called directly)
# ---------------------------------------------------------------------------

class TestParseMessageFn:

    def test_valid_json_object(self):
        msg = _make_pubsub_message(b'{"event": "page_view", "user": "u1"}')
        fn = _ParseMessageFn()
        results = list(fn.process(msg))
        assert len(results) == 1
        assert results[0]["_raw_message"] is msg
        parsed = json.loads(results[0]["payload"])
        assert parsed == {"event": "page_view", "user": "u1"}

    def test_valid_json_array(self):
        msg = _make_pubsub_message(b'[{"id": 1}, {"id": 2}]')
        fn = _ParseMessageFn()
        results = list(fn.process(msg))
        parsed = json.loads(results[0]["payload"])
        assert parsed == [{"id": 1}, {"id": 2}]

    def test_csv_row_wrapped_as_raw(self):
        csv = b"txn-001,usr-101,ord-5001,89.99"
        msg = _make_pubsub_message(csv)
        fn = _ParseMessageFn()
        results = list(fn.process(msg))
        parsed = json.loads(results[0]["payload"])
        assert "raw" in parsed
        assert "txn-001" in parsed["raw"]

    def test_invalid_utf8_wrapped_as_base64(self):
        binary = bytes([0xFF, 0xFE, 0x00, 0x01])
        msg = _make_pubsub_message(binary)
        fn = _ParseMessageFn()
        results = list(fn.process(msg))
        parsed = json.loads(results[0]["payload"])
        assert "raw_b64" in parsed

    def test_empty_json_object(self):
        msg = _make_pubsub_message(b"{}")
        fn = _ParseMessageFn()
        results = list(fn.process(msg))
        assert json.loads(results[0]["payload"]) == {}


# ---------------------------------------------------------------------------
# _AddMetadataFn — unit tests
# ---------------------------------------------------------------------------

TOPIC = "projects/shopstream-proj/topics/clickstream-topic"


class TestAddMetadataFn:

    def _run(self, payload: str, attributes: dict = None):
        msg = _make_pubsub_message(b"ignored", attributes=attributes)
        element = {"_raw_message": msg, "payload": payload}
        fn = _AddMetadataFn(TOPIC)
        return list(fn.process(element))

    def test_output_has_all_schema_fields(self):
        rows = self._run('{"x": 1}')
        assert len(rows) == 1
        row = rows[0]
        for field in ("message_id", "publish_time", "source_topic", "ingestion_time", "payload", "attributes"):
            assert field in row, f"Missing field: {field}"

    def test_source_topic_set_correctly(self):
        rows = self._run("{}")
        assert rows[0]["source_topic"] == TOPIC

    def test_message_id_propagated(self):
        rows = self._run("{}")
        assert rows[0]["message_id"] == "msg-001"

    def test_attributes_serialised_as_json(self):
        rows = self._run("{}", attributes={"env": "dev", "version": "1"})
        attrs = json.loads(rows[0]["attributes"])
        assert attrs == {"env": "dev", "version": "1"}

    def test_attributes_none_when_empty(self):
        rows = self._run("{}", attributes={})
        assert rows[0]["attributes"] is None

    def test_publish_time_contains_date(self):
        rows = self._run("{}")
        assert "2024-06-15" in rows[0]["publish_time"]

    def test_ingestion_time_is_string(self):
        rows = self._run("{}")
        assert isinstance(rows[0]["ingestion_time"], str)


# ---------------------------------------------------------------------------
# ParseMessage + AddMetadata — integration via TestPipeline
# ---------------------------------------------------------------------------

class TestPipelineIntegration:
    """
    End-to-end tests using Beam's TestPipeline (DirectRunner).
    Uses real PubsubMessage objects — MagicMock is not picklable by Beam.
    """

    def test_json_message_flows_through_both_transforms(self):
        msg = _make_real_pubsub_message(b'{"product_id": "p1", "price": 99.9}')

        with TestPipeline() as p:
            output = (
                p
                | beam.Create([msg])
                | ParseMessage()
                | AddMetadata(source_topic=TOPIC)
            )
            assert_that(
                output | beam.Map(lambda r: r["source_topic"]),
                equal_to([TOPIC]),
            )

    def test_csv_message_payload_contains_raw_key(self):
        msg = _make_real_pubsub_message(b"id,name\n1,Widget")

        with TestPipeline() as p:
            output = (
                p
                | beam.Create([msg])
                | ParseMessage()
                | AddMetadata(source_topic=TOPIC)
                | beam.Map(lambda r: json.loads(r["payload"]))
                | beam.Map(lambda d: list(d.keys())[0])
            )
            assert_that(output, equal_to(["raw"]))

    def test_multiple_messages_all_processed(self):
        messages = [
            _make_real_pubsub_message(b'{"i": 0}', message_id=f"msg-{i}")
            for i in range(5)
        ]

        with TestPipeline() as p:
            output = (
                p
                | beam.Create(messages)
                | ParseMessage()
                | AddMetadata(source_topic=TOPIC)
                | beam.Map(lambda r: r["message_id"])
            )
            assert_that(output, equal_to([f"msg-{i}" for i in range(5)]))
