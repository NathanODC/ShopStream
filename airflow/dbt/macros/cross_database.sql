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
  {{ properties_column }}.category
{%- endmacro %}

{% macro duckdb__clickstream_category(properties_column) -%}
  json_extract_string({{ properties_column }}, '$.category')
{%- endmacro %}
