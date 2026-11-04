select
  cast(user_id as {{ dbt.type_string() }}) as user_id,
  cast(billing_country as {{ dbt.type_string() }}) as billing_country,
  {{ dbt.hash('shipping_address') }} as address_hash,
  {{ dbt.hash("coalesce(cast(user_id as " ~ dbt.type_string() ~ "), '') || '|' || coalesce(cast(billing_country as " ~ dbt.type_string() ~ "), '') || '|' || coalesce(cast(shipping_address as " ~ dbt.type_string() ~ "), '')") }} as row_hash,
  dbt_valid_from as valid_from,
  dbt_valid_to as valid_to
from {{ ref('customers_snapshot') }}
