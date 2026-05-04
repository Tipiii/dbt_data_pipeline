with latest_order_details as (
    select *
    from {{ ref('sat_order_details') }}
    qualify row_number() over (
        partition by hk_order
        order by effective_from desc, load_timestamp desc
    ) = 1
)

select
    orders.hk_order,
    orders.order_id,
    order_user.hk_user,
    users.user_id,
    details.eval_set,
    cast(details.order_number as int64) as order_number,
    cast(details.order_dow as int64) as order_dow,
    case cast(details.order_dow as int64)
        when 0 then 'Sunday'
        when 1 then 'Monday'
        when 2 then 'Tuesday'
        when 3 then 'Wednesday'
        when 4 then 'Thursday'
        when 5 then 'Friday'
        when 6 then 'Saturday'
    end as order_dow_name,
    cast(details.order_hour_of_day as int64) as order_hour_of_day,
    cast(details.days_since_prior_order as float64) as days_since_prior_order,
    cast(details.order_number as int64) = 1 as is_first_order,
    orders.record_source,
    orders.load_timestamp as hub_load_timestamp,
    details.load_timestamp as sat_load_timestamp
from {{ ref('hub_order') }} as orders
left join latest_order_details as details
    on orders.hk_order = details.hk_order
left join {{ ref('link_order_user') }} as order_user
    on orders.hk_order = order_user.hk_order
left join {{ ref('hub_user') }} as users
    on order_user.hk_user = users.hk_user
