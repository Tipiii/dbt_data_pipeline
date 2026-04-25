select
    order_id,
    product_id,
    count(*) as row_count
from {{ ref('fct_order_products') }}
group by 1, 2
having count(*) > 1
