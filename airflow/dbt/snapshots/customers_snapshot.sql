{% snapshot customers_snapshot %}

{{
  config(
    target_schema='shopstream_snapshots',
    unique_key='user_id',
    strategy='check',
    check_cols=['billing_country', 'shipping_address'],
    invalidate_hard_deletes=True
  )
}}

with sales_transactions as (
  {{ normalize_streaming_sales(source('streaming_bronze', 'sales_transactions')) }}
),

ranked_customers as (
  select
    user_id,
    billing_country,
    shipping_address,
    {{ adapter.quote('timestamp') }} as source_updated_at,
    row_number() over (
      partition by user_id
      order by {{ adapter.quote('timestamp') }} desc
    ) as row_number
  from sales_transactions
  where user_id is not null
    and shipping_address is not null
)

select
  user_id,
  billing_country,
  shipping_address,
  source_updated_at
from ranked_customers
where row_number = 1

{% endsnapshot %}
