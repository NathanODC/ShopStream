CREATE OR REPLACE EXTERNAL TABLE `shopstream_bronze.sales_transactions` (
  transaction_id STRING,
  user_id STRING,
  `timestamp` TIMESTAMP,
  amount FLOAT64,
  currency STRING,
  payment_method STRING,
  status STRING,
  items STRING,
  shipping_address STRING,
  billing_country STRING,
)
WITH PARTITION COLUMNS (
  dt DATE
)
OPTIONS (
  format = 'CSV',
  uris = ['gs://shopstream-bronze-sales/pubsub-data/sales-transactions/*'],
  hive_partition_uri_prefix = 'gs://shopstream-bronze-sales/pubsub-data/sales-transactions/',
  skip_leading_rows = 1,
  allow_jagged_rows = true
);