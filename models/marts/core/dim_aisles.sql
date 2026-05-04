with latest_aisle_details as (
    select *
    from {{ ref('sat_aisle_details') }}
    qualify row_number() over (
        partition by hk_aisle
        order by effective_from desc, load_timestamp desc
    ) = 1
)

select
    hub.hk_aisle,
    hub.aisle_id,
    sat.aisle,
    hub.record_source,
    hub.load_timestamp as hub_load_timestamp,
    sat.load_timestamp as sat_load_timestamp
from {{ ref('hub_aisle') }} as hub
left join latest_aisle_details as sat
    on hub.hk_aisle = sat.hk_aisle
