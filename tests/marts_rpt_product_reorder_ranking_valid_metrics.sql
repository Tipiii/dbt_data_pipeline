select *
from {{ ref('rpt_product_reorder_ranking') }}
where product_order_count < 1
   or product_user_count < 1
   or product_line_count < product_order_count
   or reordered_line_count < 0
   or first_time_line_count < 0
   or reorder_rate < 0
   or reorder_rate > 1
   or reorder_user_penetration < 0
   or reorder_user_penetration > 1
   or first_time_purchase_rate < 0
   or first_time_purchase_rate > 1
