{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='hk_order',
    on_schema_change='sync_all_columns'
) }}

{{ build_hub(
    source_models=['stg_orders'],
    hk_column='hk_order',
    business_key_columns=['order_id']
) }}
