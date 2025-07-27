CREATE OR REPLACE EXTERNAL TABLE `shopstream_bronze.customer_support_tickets` (
  ticket_id STRING,
  user_id STRING,
  created_at TIMESTAMP,
  category STRING,
  priority STRING,
  status STRING,
  subject STRING,
  description STRING,
  sentiment_score FLOAT64,
  agent_id STRING,
  resolution_time_hours FLOAT64,
  satisfaction_score INT64,
)
WITH PARTITION COLUMNS (
  dt DATE
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  uris = ['gs://shopstream-bronze-support/pubsub-data/customer-support-topic/*'],
  hive_partition_uri_prefix = 'gs://shopstream-bronze-support/pubsub-data/customer-support-topic/'
);