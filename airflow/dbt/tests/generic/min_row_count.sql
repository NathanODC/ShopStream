{% test min_row_count(model, min_rows=1) %}

select row_count
from (select count(*) as row_count from {{ model }}) as counts
where row_count < {{ min_rows }}

{% endtest %}
