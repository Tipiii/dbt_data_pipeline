{{ build_link(
    source_models=['stg_products'],
    link_hk_column='hk_link_product_department',
    link_key_columns=['product_id', 'department_id'],
    hub_key_map={
        'hk_product': ['product_id'],
        'hk_department': ['department_id']
    }
) }}
