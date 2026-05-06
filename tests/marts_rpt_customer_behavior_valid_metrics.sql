select *
from {{ ref('rpt_customer_behavior') }}
where total_orders < 1
   or avg_basket_size < 1
   or avg_product_lines_per_order < 1
   or user_product_line_count < 1
   or user_distinct_product_count < 1
   or reordered_line_count < 0
   or first_time_line_count < 0
   or repeat_purchase_rate < 0
   or repeat_purchase_rate > 1
   or new_product_rate < 0
   or new_product_rate > 1
   or weekend_order_rate < 0
   or weekend_order_rate > 1
   or (avg_days_since_prior_order is not null and avg_days_since_prior_order < 0)
