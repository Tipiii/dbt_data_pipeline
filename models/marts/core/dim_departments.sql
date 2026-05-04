with latest_department_details as (
    select *
    from {{ ref('sat_department_details') }}
    qualify row_number() over (
        partition by hk_department
        order by effective_from desc, load_timestamp desc
    ) = 1
)

select
    hub.hk_department,
    hub.department_id,
    sat.department,
    hub.record_source,
    hub.load_timestamp as hub_load_timestamp,
    sat.load_timestamp as sat_load_timestamp
from {{ ref('hub_department') }} as hub
left join latest_department_details as sat
    on hub.hk_department = sat.hk_department
