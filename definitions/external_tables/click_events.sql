CREATE OR REPLACE EXTERNAL TABLE shopstream_bronze.clickstream_events (
  event_id STRING,
  user_id STRING,
  session_id STRING,
  timestamp TIMESTAMP,
  event_type STRING,
  page_url STRING,
  user_agent STRING,
  ip_address STRING,
  referrer STRING,
  device_type STRING,
  country STRING,
  properties STRUCT<
    product_id STRING,
    category STRING,
    price FLOAT64,
    quantity INT64,
    cart_value FLOAT64,
    items_count INT64,
    search_query STRING,
    results_count INT64
  >
)
WITH PARTITION COLUMNS (
  dt DATE
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://shopstream-bronze-events/pubsub-data/clickstream-topic/*'],
  hive_partition_uri_prefix = 'gs://shopstream-bronze-events/pubsub-data/clickstream-topic/'
);
