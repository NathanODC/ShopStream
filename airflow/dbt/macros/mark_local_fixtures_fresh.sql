{#-
  Seeds carry fixed 2026-01-01 load times, so source freshness would always
  report them as stale. Move the load times to now so `dbt source freshness`
  can be exercised end-to-end against DuckDB. Never run against BigQuery.
-#}
{% macro mark_local_fixtures_fresh() %}
  {% if target.type != 'duckdb' %}
    {{ exceptions.raise_compiler_error("mark_local_fixtures_fresh only runs against the duckdb target") }}
  {% endif %}

  {% for table in ['sales_transactions', 'clickstream_events'] %}
    {% do run_query("update " ~ source('streaming_bronze', table) ~ " set ingestion_time = current_timestamp") %}
  {% endfor %}
  {% for table in ['customer_support_tickets', 'product_catalog'] %}
    {% do run_query("update " ~ source('batch_bronze', table) ~ " set dt = current_date") %}
  {% endfor %}
  {{ log("Marked local fixture load times as fresh.", info=True) }}
{% endmacro %}
