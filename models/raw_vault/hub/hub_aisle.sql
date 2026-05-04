{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='hk_aisle',
    on_schema_change='sync_all_columns'
) }}

{{ build_hub(
    source_models=['stg_aisles'],
    hk_column='hk_aisle',
    business_key_columns=['aisle_id']
) }}
