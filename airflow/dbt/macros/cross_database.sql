{% macro flatten_sales_transactions(relation) -%}
  {{ return(adapter.dispatch('flatten_sales_transactions', 'shopstream')(relation)) }}
{%- endmacro %}

{% macro default__flatten_sales_transactions(relation) -%}
  select
    t.transaction_id,
    t.user_id,
    t.{{ adapter.quote('timestamp') }} as transaction_timestamp,
    t.status as transaction_status,
    json_value(item, '$.product_id') as product_id,
    json_value(item, '$.quantity') as quantity,
    json_value(item, '$.price') as price
  from {{ relation }} as t,
  unnest(json_query_array(t.items)) as item
{%- endmacro %}

{% macro duckdb__flatten_sales_transactions(relation) -%}
  select
    t.transaction_id,
    t.user_id,
    t.{{ adapter.quote('timestamp') }} as transaction_timestamp,
    t.status as transaction_status,
    json_extract_string(item.value, '$.product_id') as product_id,
    json_extract_string(item.value, '$.quantity') as quantity,
    json_extract_string(item.value, '$.price') as price
  from {{ relation }} as t,
  json_each(t.items) as item
{%- endmacro %}

{% macro clickstream_category(properties_column) -%}
  {{ return(adapter.dispatch('clickstream_category', 'shopstream')(properties_column)) }}
{%- endmacro %}

{% macro default__clickstream_category(properties_column) -%}
  json_value({{ properties_column }}, '$.category')
{%- endmacro %}

{% macro duckdb__clickstream_category(properties_column) -%}
  json_extract_string({{ properties_column }}, '$.category')
{%- endmacro %}

{% macro normalize_streaming_sales(relation) -%}
  {{ return(adapter.dispatch('normalize_streaming_sales', 'shopstream')(relation)) }}
{%- endmacro %}

{% macro default__normalize_streaming_sales(relation) -%}
  select
    json_value(payload, '$.transaction_id') as transaction_id,
    json_value(payload, '$.user_id') as user_id,
    cast(json_value(payload, '$.timestamp') as timestamp) as {{ adapter.quote('timestamp') }},
    json_value(payload, '$.status') as status,
    json_query(payload, '$.items') as items,
    json_value(payload, '$.shipping_address') as shipping_address,
    json_value(payload, '$.billing_country') as billing_country
  from {{ relation }}
{%- endmacro %}

{% macro duckdb__normalize_streaming_sales(relation) -%}
  select
    transaction_id,
    user_id,
    {{ adapter.quote('timestamp') }},
    status,
    items,
    shipping_address,
    billing_country
  from {{ relation }}
{%- endmacro %}

{% macro normalize_streaming_clickstream(relation) -%}
  {{ return(adapter.dispatch('normalize_streaming_clickstream', 'shopstream')(relation)) }}
{%- endmacro %}

{% macro default__normalize_streaming_clickstream(relation) -%}
  select
    json_value(payload, '$.user_id') as user_id,
    json_value(payload, '$.event_type') as event_type,
    json_query(payload, '$.properties') as properties
  from {{ relation }}
{%- endmacro %}

{% macro duckdb__normalize_streaming_clickstream(relation) -%}
  select user_id, event_type, properties
  from {{ relation }}
{%- endmacro %}
