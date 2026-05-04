{{ config(
    materialized='table',
    tags=['ml', 'reorder_prediction']
) }}

with target_orders as (
    select
        order_id,
        user_id,
        eval_set,
        order_number as target_order_number,
        order_dow as target_order_dow,
        order_hour_of_day as target_order_hour_of_day,
        coalesce(days_since_prior_order, 0.0) as target_days_since_prior_order
    from {{ ref('dim_orders') }}
    where eval_set in ('train', 'test')
),

prior_order_lines as (
    select
        fact.order_id,
        fact.user_id,
        fact.product_id,
        products.product_name,
        products.aisle_id,
        products.aisle,
        products.department_id,
        products.department,
        fact.order_number,
        fact.order_dow,
        fact.order_hour_of_day,
        coalesce(fact.days_since_prior_order, 0.0) as days_since_prior_order,
        fact.add_to_cart_order,
        fact.reordered,
        fact.basket_size
    from {{ ref('fct_order_products') }} as fact
    left join {{ ref('dim_products') }} as products
        on fact.hk_product = products.hk_product
    where fact.eval_set = 'prior'
),

prior_orders as (
    select distinct
        order_id,
        user_id,
        order_number,
        order_dow,
        order_hour_of_day,
        days_since_prior_order,
        basket_size
    from prior_order_lines
),

user_prior_stats as (
    select
        user_id,
        count(*) as user_prior_order_count,
        avg(cast(basket_size as float64)) as user_avg_basket_size,
        avg(days_since_prior_order) as user_avg_days_since_prior_order,
        avg(case when order_dow in (0, 6) then 1.0 else 0.0 end) as user_weekend_order_rate,
        avg(cast(order_hour_of_day as float64)) as user_avg_order_hour
    from prior_orders
    group by 1
),

user_prior_product_stats as (
    select
        user_id,
        count(*) as user_prior_line_count,
        count(distinct product_id) as user_prior_distinct_product_count,
        avg(cast(reordered as float64)) as user_prior_reorder_rate
    from prior_order_lines
    group by 1
),

product_prior_stats as (
    select
        product_id,
        min(product_name) as product_name,
        min(aisle_id) as aisle_id,
        min(aisle) as aisle,
        min(department_id) as department_id,
        min(department) as department,
        count(distinct order_id) as product_prior_order_count,
        count(distinct user_id) as product_prior_user_count,
        avg(cast(reordered as float64)) as product_prior_reorder_rate,
        avg(cast(add_to_cart_order as float64)) as product_avg_add_to_cart_order,
        avg(cast(basket_size as float64)) as product_avg_basket_size
    from prior_order_lines
    group by 1
),

user_product_prior_stats as (
    select
        user_id,
        product_id,
        min(product_name) as product_name,
        min(aisle_id) as aisle_id,
        min(aisle) as aisle,
        min(department_id) as department_id,
        min(department) as department,
        count(distinct order_id) as up_order_count,
        sum(case when reordered = 1 then 1 else 0 end) as up_reordered_line_count,
        min(order_number) as up_first_order_number,
        max(order_number) as up_last_order_number,
        avg(cast(add_to_cart_order as float64)) as up_avg_add_to_cart_order,
        avg(cast(basket_size as float64)) as up_avg_basket_size,
        avg(days_since_prior_order) as up_avg_days_since_prior_order,
        avg(cast(reordered as float64)) as up_reorder_rate
    from prior_order_lines
    group by 1, 2
),

user_department_prior_stats as (
    select
        user_id,
        department_id,
        count(distinct order_id) as ud_order_count,
        count(*) as ud_line_count,
        avg(cast(reordered as float64)) as ud_reorder_rate
    from prior_order_lines
    group by 1, 2
),

user_aisle_prior_stats as (
    select
        user_id,
        aisle_id,
        count(distinct order_id) as ua_order_count,
        count(*) as ua_line_count,
        avg(cast(reordered as float64)) as ua_reorder_rate
    from prior_order_lines
    group by 1, 2
),

train_labels as (
    select distinct
        order_id,
        product_id,
        1 as label
    from {{ ref('fct_order_products') }}
    where eval_set = 'train'
),

candidate_rows as (
    select
        concat(cast(target.order_id as string), '|', cast(up.product_id as string)) as candidate_key,
        target.order_id,
        target.user_id,
        up.product_id,
        up.product_name,
        up.aisle_id,
        up.aisle,
        up.department_id,
        up.department,
        target.eval_set,
        target.target_order_number,
        target.target_order_dow,
        target.target_order_hour_of_day,
        target.target_days_since_prior_order,
        cast(target.target_order_dow in (0, 6) as int64) as target_is_weekend,
        coalesce(user_orders.user_prior_order_count, 0) as user_prior_order_count,
        coalesce(user_products.user_prior_line_count, 0) as user_prior_line_count,
        coalesce(user_products.user_prior_distinct_product_count, 0) as user_prior_distinct_product_count,
        coalesce(user_products.user_prior_reorder_rate, 0.0) as user_prior_reorder_rate,
        coalesce(user_orders.user_avg_basket_size, 0.0) as user_avg_basket_size,
        coalesce(user_orders.user_avg_days_since_prior_order, 0.0) as user_avg_days_since_prior_order,
        coalesce(user_orders.user_weekend_order_rate, 0.0) as user_weekend_order_rate,
        coalesce(user_orders.user_avg_order_hour, 0.0) as user_avg_order_hour,
        coalesce(product_stats.product_prior_order_count, 0) as product_prior_order_count,
        coalesce(product_stats.product_prior_user_count, 0) as product_prior_user_count,
        coalesce(product_stats.product_prior_reorder_rate, 0.0) as product_prior_reorder_rate,
        coalesce(product_stats.product_avg_add_to_cart_order, 0.0) as product_avg_add_to_cart_order,
        coalesce(product_stats.product_avg_basket_size, 0.0) as product_avg_basket_size,
        up.up_order_count,
        up.up_reordered_line_count,
        up.up_first_order_number,
        up.up_last_order_number,
        coalesce(up.up_avg_add_to_cart_order, 0.0) as up_avg_add_to_cart_order,
        coalesce(up.up_avg_basket_size, 0.0) as up_avg_basket_size,
        coalesce(up.up_avg_days_since_prior_order, 0.0) as up_avg_days_since_prior_order,
        coalesce(up.up_reorder_rate, 0.0) as up_reorder_rate,
        safe_divide(up.up_order_count, nullif(user_orders.user_prior_order_count, 0)) as up_order_share,
        target.target_order_number - up.up_last_order_number as up_orders_since_last_purchase,
        target.target_order_number - up.up_first_order_number as up_customer_product_lifetime,
        coalesce(user_department.ud_order_count, 0) as ud_order_count,
        coalesce(user_department.ud_line_count, 0) as ud_line_count,
        coalesce(user_department.ud_reorder_rate, 0.0) as ud_reorder_rate,
        safe_divide(user_department.ud_order_count, nullif(user_orders.user_prior_order_count, 0)) as ud_order_share,
        coalesce(user_aisle.ua_order_count, 0) as ua_order_count,
        coalesce(user_aisle.ua_line_count, 0) as ua_line_count,
        coalesce(user_aisle.ua_reorder_rate, 0.0) as ua_reorder_rate,
        safe_divide(user_aisle.ua_order_count, nullif(user_orders.user_prior_order_count, 0)) as ua_order_share,
        case
            when target.eval_set = 'train' then coalesce(labels.label, 0)
        end as label
    from target_orders as target
    inner join user_product_prior_stats as up
        on target.user_id = up.user_id
    left join user_prior_stats as user_orders
        on target.user_id = user_orders.user_id
    left join user_prior_product_stats as user_products
        on target.user_id = user_products.user_id
    left join product_prior_stats as product_stats
        on up.product_id = product_stats.product_id
    left join user_department_prior_stats as user_department
        on target.user_id = user_department.user_id
       and up.department_id = user_department.department_id
    left join user_aisle_prior_stats as user_aisle
        on target.user_id = user_aisle.user_id
       and up.aisle_id = user_aisle.aisle_id
    left join train_labels as labels
        on target.order_id = labels.order_id
       and up.product_id = labels.product_id
)

select *
from candidate_rows
