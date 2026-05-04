select *
from {{ ref('rpt_product_reorder_ranking') }}
where product_order_count < 1
   or reordered_line_count < 0
   or reorder_rate < 0
   or reorder_rate > 1
