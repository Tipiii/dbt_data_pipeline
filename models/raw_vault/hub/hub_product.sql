{{ build_hub(
    source_models=['stg_products'],
    hk_column='hk_product',
    business_key_columns=['product_id']
) }}
