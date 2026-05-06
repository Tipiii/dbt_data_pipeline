with base as (
    select
        product_id,
        product_name,
        aisle_id,
        aisle,
        department_id,
        department,
        order_id,
        user_id,
        order_number,
        add_to_cart_order,
        basket_size,
        days_since_prior_order,
        reordered,
        is_reordered
    from {{ ref('fct_order_products') }}
    where eval_set in ('prior', 'train')
),

product_metrics as (
    select
        product_id,
        min(product_name) as product_name,
        aisle_id,
        min(aisle) as aisle,
        department_id,
        min(department) as department,
        count(*) as product_line_count,
        count(distinct order_id) as product_order_count,
        count(distinct user_id) as product_user_count,
        sum(case when reordered = 1 then 1 else 0 end) as reordered_line_count,
        sum(case when reordered = 0 then 1 else 0 end) as first_time_line_count,
        sum(case when add_to_cart_order = 1 then 1 else 0 end) as first_cart_line_count,
        count(distinct case when reordered = 1 then user_id end) as reordered_user_count,
        avg(add_to_cart_order) as avg_add_to_cart_order,
        avg(basket_size) as avg_basket_size,
        avg(days_since_prior_order) as avg_days_since_prior_order
    from base
    group by 1, 3, 5
),

scored as (
    select
        *,
        safe_divide(reordered_line_count, product_line_count) as reorder_rate,
        safe_divide(reordered_user_count, product_user_count) as reorder_user_penetration,
        safe_divide(first_time_line_count, product_line_count) as first_time_purchase_rate,
        safe_divide(first_cart_line_count, product_line_count) as first_cart_rate,
        safe_divide(product_order_count, product_user_count) as orders_per_user,
        safe_divide(product_line_count, product_order_count) as avg_lines_per_order,
        safe_divide(reordered_line_count, nullif(first_time_line_count, 0)) as reorder_to_first_time_ratio,
        safe_divide(reordered_line_count, product_line_count) * ln(product_order_count + 1) as weighted_reorder_score,
        product_order_count >= 10 as meets_min_orders_10,
        product_order_count >= 50 as meets_min_orders_50
    from product_metrics
)

select
    product_id,
    product_name,
    aisle_id,
    aisle,
    department_id,
    department,
    product_line_count,
    product_order_count,
    product_user_count,
    reordered_line_count,
    first_time_line_count,
    reorder_rate,
    reorder_user_penetration,
    first_time_purchase_rate,
    first_cart_line_count,
    first_cart_rate,
    avg_add_to_cart_order,
    avg_basket_size,
    avg_days_since_prior_order,
    orders_per_user,
    avg_lines_per_order,
    reorder_to_first_time_ratio,
    weighted_reorder_score,
    meets_min_orders_10,
    meets_min_orders_50,
    rank() over (
        order by reorder_rate desc, product_order_count desc, product_id
    ) as reorder_rate_rank_overall,
    rank() over (
        order by weighted_reorder_score desc, product_order_count desc, product_id
    ) as weighted_reorder_rank_overall,
    rank() over (
        order by product_order_count desc, reorder_rate desc, product_id
    ) as demand_rank_overall
from scored
