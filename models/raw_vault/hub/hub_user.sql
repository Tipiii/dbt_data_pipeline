{{ build_hub(
    source_models=['stg_orders'],
    hk_column='hk_user',
    business_key_columns=['user_id']
) }}
