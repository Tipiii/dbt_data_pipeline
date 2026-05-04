with latest_order_product_details as (
    select *
    from {{ ref('sat_order_product_details') }}
    qualify row_number() over (
        partition by hk_link_order_product
        order by effective_from desc, load_timestamp desc
    ) = 1
)

select
    order_products.hk_link_order_product,
    order_products.hk_order,
    order_products.hk_product,
    orders.hk_user,
    products.hk_aisle,
    products.hk_department,
    orders.order_id,
    orders.user_id,
    products.product_id,
    products.product_name,
    products.aisle_id,
    products.aisle,
    products.department_id,
    products.department,
    orders.eval_set,
    orders.order_number,
    orders.order_dow,
    orders.order_dow_name,
    orders.order_hour_of_day,
    orders.days_since_prior_order,
    cast(details.add_to_cart_order as int64) as add_to_cart_order,
    cast(details.reordered as int64) as reordered,
    cast(details.reordered as int64) = 1 as is_reordered,
    count(*) over (partition by order_products.hk_order) as basket_size,
    order_products.record_source,
    order_products.load_timestamp as link_load_timestamp,
    details.load_timestamp as sat_load_timestamp
from {{ ref('link_order_product') }} as order_products
left join latest_order_product_details as details
    on order_products.hk_link_order_product = details.hk_link_order_product
left join {{ ref('dim_orders') }} as orders
    on order_products.hk_order = orders.hk_order
left join {{ ref('dim_products') }} as products
    on order_products.hk_product = products.hk_product
