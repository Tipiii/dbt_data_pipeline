with user_order_stats as (
    select
        order_user.hk_user,
        count(distinct orders.hk_order) as total_orders,
        min(orders.order_number) as first_order_number,
        max(orders.order_number) as latest_order_number,
        avg(orders.days_since_prior_order) as avg_days_since_prior_order
    from {{ ref('link_order_user') }} as order_user
    left join {{ ref('dim_orders') }} as orders
        on order_user.hk_order = orders.hk_order
    group by 1
)

select
    users.hk_user,
    users.user_id,
    coalesce(stats.total_orders, 0) as total_orders,
    stats.first_order_number,
    stats.latest_order_number,
    stats.avg_days_since_prior_order,
    users.record_source,
    users.load_timestamp as hub_load_timestamp
from {{ ref('hub_user') }} as users
left join user_order_stats as stats
    on users.hk_user = stats.hk_user
