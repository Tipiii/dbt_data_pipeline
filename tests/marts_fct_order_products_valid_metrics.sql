select *
from {{ ref('fct_order_products') }}
where add_to_cart_order < 1
   or basket_size < 1
   or add_to_cart_order > basket_size
   or reordered not in (0, 1)
   or is_reordered != (reordered = 1)
