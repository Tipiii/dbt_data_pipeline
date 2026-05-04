{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['hk_department', 'hashdiff'],
    on_schema_change='sync_all_columns'
) }}

{{ build_sat(
    source_models=['stg_departments'],
    parent_hk_column='hk_department',
    parent_key_columns=['department_id'],
    hashdiff_columns_list=['department'],
    payload_columns=['department']
) }}
