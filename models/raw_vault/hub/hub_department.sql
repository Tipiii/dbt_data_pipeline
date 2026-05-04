{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='hk_department',
    on_schema_change='sync_all_columns'
) }}

    
{{ build_hub(
    source_models=['stg_departments'],
    hk_column='hk_department',
    business_key_columns=['department_id']
) }}
