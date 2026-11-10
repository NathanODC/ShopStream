with user_purchase_patterns as (
  select
    user_id,
    count(distinct transaction_id) as total_orders,
    sum(total_amount) as total_lifetime_spend,
    avg(total_amount) as average_order_value,
    max(transaction_timestamp) as last_purchase_date,
    count(distinct product_id) as distinct_products_purchased
  from {{ ref('f_sales') }}
  group by user_id
),

clickstream_events as (
  select * from {{ ref('stg_clickstream_events') }}
),

user_browsing_history as (
  select
    user_id,
    array_agg(distinct {{ clickstream_category('properties') }}) as browsed_categories
  from clickstream_events
  where event_type = 'page_view'
    and {{ clickstream_category('properties') }} is not null
  group by user_id
)

select
  purchases.user_id,
  customers.billing_country,
  purchases.total_orders,
  purchases.total_lifetime_spend,
  purchases.average_order_value,
  purchases.last_purchase_date,
  purchases.distinct_products_purchased,
  browsing.browsed_categories
from user_purchase_patterns as purchases
inner join {{ ref('d_customers') }} as customers
  on purchases.user_id = customers.user_id
  and customers.valid_to is null
left join user_browsing_history as browsing
  on purchases.user_id = browsing.user_id
