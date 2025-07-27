CREATE OR REPLACE EXTERNAL TABLE `shopstream_bronze.product_catalog` (
  product_id STRING,
  name STRING,
  category STRING,
  subcategory STRING,
  price FLOAT64,
  cost FLOAT64,
  stock_quantity INT64,
  supplier_id STRING,
  last_updated TIMESTAMP,
  attributes JSON,
  tags ARRAY<STRING>,
  dt DATE
)
WITH PARTITION COLUMNS (
  dt DATE
)
OPTIONS (
  format = 'NEWLINE_DELIMITED_JSON',
  
  uris = ['gs://shopstream-bronze-products/pubsub-data/product-catalog/*'],
  hive_partition_uri_prefix = 'gs://shopstream-bronze-products/pubsub-data/product-catalog/'
);