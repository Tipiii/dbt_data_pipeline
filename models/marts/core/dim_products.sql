with latest_product_details as (
    select *
    from {{ ref('sat_product_details') }}
    qualify row_number() over (
        partition by hk_product
        order by effective_from desc, load_timestamp desc
    ) = 1
),

latest_product_aisle as (
    select *
    from {{ ref('link_product_aisle') }}
    qualify row_number() over (
        partition by hk_product
        order by load_timestamp desc
    ) = 1
),

latest_product_department as (
    select *
    from {{ ref('link_product_department') }}
    qualify row_number() over (
        partition by hk_product
        order by load_timestamp desc
    ) = 1
)

select
    products.hk_product,
    products.product_id,
    details.product_name,
    product_aisle.hk_aisle,
    aisles.aisle_id,
    aisles.aisle,
    product_department.hk_department,
    departments.department_id,
    departments.department,
    products.record_source,
    products.load_timestamp as hub_load_timestamp,
    details.load_timestamp as sat_load_timestamp
from {{ ref('hub_product') }} as products
left join latest_product_details as details
    on products.hk_product = details.hk_product
left join latest_product_aisle as product_aisle
    on products.hk_product = product_aisle.hk_product
left join {{ ref('dim_aisles') }} as aisles
    on product_aisle.hk_aisle = aisles.hk_aisle
left join latest_product_department as product_department
    on products.hk_product = product_department.hk_product
left join {{ ref('dim_departments') }} as departments
    on product_department.hk_department = departments.hk_department
