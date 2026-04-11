{{ build_hub(
    source_models=['stg_orders'],
    hk_column='hk_order',
    business_key_columns=['order_id']
) }}
