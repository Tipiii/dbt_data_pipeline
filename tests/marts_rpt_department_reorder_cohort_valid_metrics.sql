select *
from {{ ref('rpt_department_reorder_cohort') }}
where first_department_order_number < 1
   or orders_since_first_department_order < 0
   or cohort_user_count < 1
   or active_user_count < 1
   or active_user_count > cohort_user_count
   or active_user_retention_rate < 0
   or active_user_retention_rate > 1
