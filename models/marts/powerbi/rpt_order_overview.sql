with order_lines as (
    select
        order_id,
        user_id,
        eval_set,
        order_number,
        order_dow,
        order_dow_name,
        order_hour_of_day,
        days_since_prior_order,
        product_id,
        aisle_id,
        department_id,
        add_to_cart_order,
        reordered,
        basket_size
    from {{ ref('fct_order_products') }}
    where eval_set in ('prior', 'train')
),

order_summary as (
    select
        order_id,
        user_id,
        min(eval_set) as eval_set,
        min(order_number) as order_number,
        min(order_dow) as order_dow,
        min(order_dow_name) as order_dow_name,
        min(order_hour_of_day) as order_hour_of_day,
        min(days_since_prior_order) as days_since_prior_order,
        count(*) as product_line_count,
        count(distinct product_id) as distinct_product_count,
        count(distinct aisle_id) as distinct_aisle_count,
        count(distinct department_id) as distinct_department_count,
        max(basket_size) as basket_size,
        sum(case when reordered = 1 then 1 else 0 end) as reordered_line_count,
        sum(case when reordered = 0 then 1 else 0 end) as first_time_line_count,
        avg(cast(add_to_cart_order as float64)) as avg_add_to_cart_order
    from order_lines
    group by 1, 2
)

select
    cast(order_id as string) as order_row_key,
    order_id,
    user_id,
    eval_set,
    order_number,
    order_dow,
    order_dow_name,
    order_hour_of_day,
    days_since_prior_order,
    order_number = 1 as is_first_order,
    order_dow in (0, 6) as is_weekend,
    product_line_count,
    distinct_product_count,
    distinct_aisle_count,
    distinct_department_count,
    basket_size,
    reordered_line_count,
    first_time_line_count,
    safe_divide(reordered_line_count, product_line_count) as reorder_rate,
    avg_add_to_cart_order
from order_summary
