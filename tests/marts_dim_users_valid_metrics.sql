select *
from {{ ref('dim_users') }}
where total_orders < 0
   or first_order_number < 1
   or latest_order_number < 1
   or latest_order_number < first_order_number
   or avg_days_since_prior_order < 0
   or (
        total_orders > 0
        and (
            first_order_number is null
            or latest_order_number is null
        )
    )
