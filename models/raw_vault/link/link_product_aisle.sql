{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='hk_link_product_aisle',
    on_schema_change='sync_all_columns'
) }}

{{ build_link(
    source_models=['stg_products'],
    link_hk_column='hk_link_product_aisle',
    link_key_columns=['product_id', 'aisle_id'],
    hub_key_map={
        'hk_product': ['product_id'],
        'hk_aisle': ['aisle_id']
    }
) }}
