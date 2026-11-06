with latest_partition as (
  select max(dt) as dt
  from {{ source('batch_bronze', 'product_catalog') }}
),

final as (
  select
    cast(product_id as {{ dbt.type_string() }}) as product_id,
    name,
    category,
    cast(price as {{ dbt.type_float() }}) as price,
    cast(cost as {{ dbt.type_float() }}) as cost,
    cast(stock_quantity as {{ dbt.type_int() }}) as stock_quantity,
    supplier_id,
    attributes,
    last_updated,
    tags
  from {{ source('batch_bronze', 'product_catalog') }}
  where dt = (select dt from latest_partition)
)

select * from final
