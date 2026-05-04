with base as (
    select distinct
        user_id,
        order_id,
        order_number,
        department_id,
        department
    from {{ ref('fct_order_products') }}
    where eval_set in ('prior', 'train')
),

first_department_orders as (
    select
        user_id,
        department_id,
        min(order_number) as first_department_order_number
    from base
    group by 1, 2
),

cohort_sizes as (
    select
        first_orders.department_id,
        min(base.department) as department,
        first_orders.first_department_order_number,
        count(distinct first_orders.user_id) as cohort_user_count
    from first_department_orders as first_orders
    inner join base
        on first_orders.user_id = base.user_id
       and first_orders.department_id = base.department_id
    group by 1, 3
),

cohort_activity as (
    select
        base.department_id,
        min(base.department) as department,
        first_orders.first_department_order_number,
        base.order_number - first_orders.first_department_order_number as orders_since_first_department_order,
        count(distinct base.user_id) as active_user_count,
        count(distinct case
            when base.order_number > first_orders.first_department_order_number then base.user_id
        end) as returning_user_count,
        count(distinct base.order_id) as active_order_count,
        count(*) as department_line_count
    from base
    inner join first_department_orders as first_orders
        on base.user_id = first_orders.user_id
       and base.department_id = first_orders.department_id
    group by 1, 3, 4
)

select
    concat(
        cast(activity.department_id as string), '|',
        cast(activity.first_department_order_number as string), '|',
        cast(activity.orders_since_first_department_order as string)
    ) as cohort_row_key,
    activity.department_id,
    activity.department,
    activity.first_department_order_number,
    activity.orders_since_first_department_order,
    sizes.cohort_user_count,
    activity.active_user_count,
    activity.returning_user_count,
    safe_divide(activity.active_user_count, sizes.cohort_user_count) as active_user_retention_rate,
    safe_divide(activity.returning_user_count, sizes.cohort_user_count) as returning_user_rate,
    activity.active_order_count,
    activity.department_line_count
from cohort_activity as activity
inner join cohort_sizes as sizes
    on activity.department_id = sizes.department_id
   and activity.first_department_order_number = sizes.first_department_order_number
