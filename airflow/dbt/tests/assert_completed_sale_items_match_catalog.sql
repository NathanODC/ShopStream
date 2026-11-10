-- f_sales inner-joins the latest catalog, so completed items whose product is
-- missing from it are silently dropped. Surface them instead.
{{ config(severity='warn', tags=['silver']) }}

with sales_items as (
  {{ flatten_sales_transactions(ref('stg_sales_transactions')) }}
)

select sales_items.transaction_id, sales_items.product_id
from sales_items
left join {{ ref('d_products') }} as products
  on sales_items.product_id = products.product_id
where sales_items.transaction_status = 'completed'
  and sales_items.user_id is not null
  and products.product_id is null
