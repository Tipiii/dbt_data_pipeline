{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['hk_product', 'hashdiff'],
    on_schema_change='sync_all_columns'
) }}

{{ build_sat(
    source_models=['stg_products'],
    parent_hk_column='hk_product',
    parent_key_columns=['product_id'],
    hashdiff_columns_list=['product_name'],
    payload_columns=['product_name']
) }}
