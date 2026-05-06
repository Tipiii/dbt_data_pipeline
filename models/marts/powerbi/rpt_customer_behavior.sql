with order_summary as (
    select distinct
        order_id,
        user_id,
        order_number,
        order_dow,
        order_hour_of_day,
        days_since_prior_order,
        basket_size
    from {{ ref('fct_order_products') }}
    where eval_set in ('prior', 'train')
),

user_order_metrics as (
    select
        user_id,
        count(*) as total_orders,
        min(order_number) as first_order_number,
        max(order_number) as latest_order_number,
        avg(days_since_prior_order) as avg_days_since_prior_order,
        avg(cast(basket_size as float64)) as avg_basket_size,
        avg(cast(order_hour_of_day as float64)) as avg_order_hour,
        avg(case when order_dow in (0, 6) then 1.0 else 0.0 end) as weekend_order_rate
    from order_summary
    group by 1
),

user_line_metrics as (
    select
        user_id,
        count(*) as user_product_line_count,
        count(distinct product_id) as user_distinct_product_count,
        count(distinct aisle_id) as user_distinct_aisle_count,
        count(distinct department_id) as user_distinct_department_count,
        sum(case when reordered = 1 then 1 else 0 end) as reordered_line_count,
        sum(case when reordered = 0 then 1 else 0 end) as first_time_line_count,
        count(distinct case when reordered = 1 then product_id end) as reordered_distinct_product_count
    from {{ ref('fct_order_products') }}
    where eval_set in ('prior', 'train')
    group by 1
)

select
    orders.user_id,
    orders.total_orders,
    orders.first_order_number,
    orders.latest_order_number,
    orders.avg_days_since_prior_order,
    orders.avg_basket_size,
    safe_divide(lines.user_product_line_count, orders.total_orders) as avg_product_lines_per_order,
    orders.avg_order_hour,
    orders.weekend_order_rate,
    lines.user_product_line_count,
    lines.user_distinct_product_count,
    lines.user_distinct_aisle_count,
    lines.user_distinct_department_count,
    lines.reordered_line_count,
    lines.first_time_line_count,
    safe_divide(lines.reordered_line_count, lines.user_product_line_count) as repeat_purchase_rate,
    safe_divide(lines.first_time_line_count, lines.user_product_line_count) as new_product_rate,
    safe_divide(
        lines.reordered_distinct_product_count,
        nullif(lines.user_distinct_product_count, 0)
    ) as repeat_product_coverage,
    safe_divide(lines.reordered_line_count, nullif(lines.first_time_line_count, 0)) as repeat_to_new_ratio,
    case
        when orders.total_orders between 1 and 5 then '01_05_orders'
        when orders.total_orders between 6 and 10 then '06_10_orders'
        when orders.total_orders between 11 and 20 then '11_20_orders'
        else '21_plus_orders'
    end as customer_order_segment,
    case
        when orders.avg_days_since_prior_order is null then 'single_order'
        when orders.avg_days_since_prior_order <= 7 then 'high_frequency'
        when orders.avg_days_since_prior_order <= 14 then 'medium_frequency'
        else 'low_frequency'
    end as customer_frequency_segment,
    case
        when lines.reordered_line_count > lines.first_time_line_count then 'repeat_leaning'
        when lines.reordered_line_count < lines.first_time_line_count then 'exploration_leaning'
        else 'balanced'
    end as product_preference_segment
from user_order_metrics as orders
inner join user_line_metrics as lines
    on orders.user_id = lines.user_id
