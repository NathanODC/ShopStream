"""
Dataflow pipeline configuration.

Defines the BigQuery schema used for all topics and a convenience map from
topic path → default BQ table name.  Because final per-topic schemas are not
yet defined, every topic writes to a generic table that stores the raw
payload as a JSON string.  Dataform silver transforms are responsible for
parsing `payload` into typed columns.
"""

# ---------------------------------------------------------------------------
# Generic BigQuery schema
# Each row represents one Pub/Sub message, regardless of the source topic.
# ---------------------------------------------------------------------------

BQ_SCHEMA = {
    "fields": [
        {"name": "message_id",     "type": "STRING",    "mode": "REQUIRED"},
        {"name": "publish_time",   "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "source_topic",   "type": "STRING",    "mode": "REQUIRED"},
        {"name": "ingestion_time", "type": "TIMESTAMP", "mode": "REQUIRED"},
        # Raw message content stored as a JSON string.
        # Replace with typed fields once topic schemas are finalised.
        {"name": "payload",        "type": "STRING",    "mode": "REQUIRED"},
        # Pub/Sub message attributes (key/value pairs) serialised as JSON.
        {"name": "attributes",     "type": "STRING",    "mode": "NULLABLE"},
    ]
}

# ---------------------------------------------------------------------------
# Topic → default BQ table name
# Override at runtime with --bq_table if a different table is desired.
# ---------------------------------------------------------------------------

TOPIC_TABLE_MAP: dict[str, str] = {
    "projects/shopstream-proj/topics/clickstream-topic":      "clickstream_raw",
    "projects/shopstream-proj/topics/customer-support-topic": "customer_support_raw",
    "projects/shopstream-proj/topics/product-catalog":        "product_catalog_raw",
    "projects/shopstream-proj/topics/sales-transactions":     "sales_transactions_raw",
}

# Default BigQuery dataset that holds the streaming tables.
DEFAULT_BQ_DATASET = "shopstream_streaming"
