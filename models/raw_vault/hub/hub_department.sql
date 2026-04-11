    
{{ build_hub(
    source_models=['stg_departments'],
    hk_column='hk_department',
    business_key_columns=['department_id']
) }}
