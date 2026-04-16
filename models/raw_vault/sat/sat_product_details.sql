{{ build_sat(
    source_models=['stg_products'],
    parent_hk_column='hk_product',
    parent_key_columns=['product_id'],
    hashdiff_columns_list=['product_name'],
    payload_columns=['product_name']
) }}
