-- Every completed sale must be reflected once in the gold customer summary.
{{ config(tags=['gold']) }}

with fact_totals as (
  select
    count(distinct transaction_id) as total_orders,
    sum(total_amount) as total_spend
  from {{ ref('f_sales') }}
),

gold_totals as (
  select
    sum(total_orders) as total_orders,
    sum(total_lifetime_spend) as total_spend
  from {{ ref('user_behavior_summary') }}
)

select
  fact_totals.total_orders as fact_orders,
  gold_totals.total_orders as gold_orders,
  fact_totals.total_spend as fact_spend,
  gold_totals.total_spend as gold_spend
from fact_totals
cross join gold_totals
where fact_totals.total_orders != coalesce(gold_totals.total_orders, 0)
  or abs(fact_totals.total_spend - coalesce(gold_totals.total_spend, 0)) > 0.01
