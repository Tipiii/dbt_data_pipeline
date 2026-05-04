select *
from {{ ref('rpt_product_affinity') }}
where product_id_a = product_id_b
   or pair_order_count < 1
   or pair_user_count < 1
   or confidence_a_to_b < 0
   or confidence_a_to_b > 1
   or confidence_b_to_a < 0
   or confidence_b_to_a > 1
   or support_rate < 0
   or support_rate > 1
   or lift < 0
