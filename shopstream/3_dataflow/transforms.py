"""
Apache Beam PTransforms for the ShopStream Dataflow streaming pipeline.

Two transforms form the core of the pipeline:

  ParseMessage  — converts raw Pub/Sub message bytes into a Python dict,
                  handling both JSON and non-JSON (e.g. CSV) payloads.

  AddMetadata   — enriches the parsed dict with Pub/Sub envelope fields
                  (message_id, publish_time) and a server-side ingestion
                  timestamp, producing rows ready for BigQuery.

Keeping transforms in a dedicated module makes them independently testable
with beam.testing.TestPipeline and reusable across pipeline variants.
"""

import json
import logging
from datetime import datetime, timezone

import apache_beam as beam

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# ParseMessage
# ---------------------------------------------------------------------------

class _ParseMessageFn(beam.DoFn):
    """
    Parses raw Pub/Sub message bytes into a plain Python dict.

    Behaviour:
    - If the message data is valid UTF-8 JSON → store as-is (re-serialised to
      a compact JSON string).
    - If the message data is any other bytes (CSV, Avro, binary …) → wrap in
      {"raw": "<base64-or-string>"} so the payload field is always a valid
      JSON string, and the row is never dropped.

    Emits one dict per message with keys:
        _raw_message   – the original beam.io.PubsubMessage object
        payload        – JSON string
    """

    def process(self, message: beam.io.PubsubMessage):  # type: ignore[name-defined]
        try:
            text = message.data.decode("utf-8")
        except UnicodeDecodeError:
            import base64
            text = base64.b64encode(message.data).decode("ascii")
            payload = json.dumps({"raw_b64": text})
        else:
            try:
                parsed = json.loads(text)
                payload = json.dumps(parsed, ensure_ascii=False)
            except json.JSONDecodeError:
                # Non-JSON text (e.g. a CSV row)
                payload = json.dumps({"raw": text})

        yield {
            "_raw_message": message,
            "payload": payload,
        }


class ParseMessage(beam.PTransform):
    """Wraps _ParseMessageFn for cleaner pipeline graph labelling."""

    def expand(self, pcoll):
        return pcoll | "ParseMessageBytes" >> beam.ParDo(_ParseMessageFn())


# ---------------------------------------------------------------------------
# AddMetadata
# ---------------------------------------------------------------------------

class _AddMetadataFn(beam.DoFn):
    """
    Adds Pub/Sub envelope fields and a server-side ingestion timestamp.

    Input  : dict with keys ``_raw_message`` and ``payload``
    Output : dict matching the generic BQ_SCHEMA defined in config.py
    """

    def __init__(self, source_topic: str):
        self._source_topic = source_topic

    def process(self, element: dict):
        msg: beam.io.PubsubMessage = element["_raw_message"]  # type: ignore[name-defined]

        publish_time: datetime = msg.publish_time  # type: ignore[attr-defined]
        if publish_time is None:
            publish_time = datetime.now(tz=timezone.utc)

        attributes = msg.attributes if msg.attributes else {}

        yield {
            "message_id":     msg.message_id,
            "publish_time":   publish_time.strftime("%Y-%m-%d %H:%M:%S.%f UTC"),
            "source_topic":   self._source_topic,
            "ingestion_time": datetime.now(tz=timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S.%f UTC"
            ),
            "payload":    element["payload"],
            "attributes": json.dumps(dict(attributes)) if attributes else None,
        }


class AddMetadata(beam.PTransform):
    """Enriches parsed messages with Pub/Sub and pipeline metadata."""

    def __init__(self, source_topic: str, label: str = "AddMetadata"):
        super().__init__(label)
        self._source_topic = source_topic

    def expand(self, pcoll):
        return pcoll | "AttachMetadata" >> beam.ParDo(
            _AddMetadataFn(self._source_topic)
        )
