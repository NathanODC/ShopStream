with flattened_transactions as (
  {{ flatten_sales_transactions(source('bronze', 'sales_transactions')) }}
),

completed_sales as (
  select *
  from flattened_transactions
  where user_id is not null
    and transaction_status = 'completed'
)

select
  sales.transaction_id,
  sales.user_id,
  products.product_id,
  sales.transaction_timestamp,
  cast(sales.quantity as {{ dbt.type_int() }}) as quantity,
  cast(sales.price as {{ dbt.type_float() }}) as price,
  cast(sales.quantity as {{ dbt.type_int() }})
    * cast(sales.price as {{ dbt.type_float() }}) as total_amount
from completed_sales as sales
inner join {{ ref('d_products') }} as products
  on sales.product_id = products.product_id
