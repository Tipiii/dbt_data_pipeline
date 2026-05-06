select *
from {{ ref('rpt_order_overview') }}
where order_number < 1
   or product_line_count < 1
   or distinct_product_count < 1
   or distinct_aisle_count < 1
   or distinct_department_count < 1
   or basket_size < 1
   or reordered_line_count < 0
   or first_time_line_count < 0
   or reordered_line_count + first_time_line_count != product_line_count
   or reorder_rate < 0
   or reorder_rate > 1
