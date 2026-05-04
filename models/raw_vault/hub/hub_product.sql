{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='hk_product',
    on_schema_change='sync_all_columns'
) }}

{{ build_hub(
    source_models=['stg_products'],
    hk_column='hk_product',
    business_key_columns=['product_id']
) }}
