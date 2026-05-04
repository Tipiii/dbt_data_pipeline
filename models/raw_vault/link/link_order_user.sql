{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='hk_link_order_user',
    on_schema_change='sync_all_columns'
) }}

{{ build_link(
    source_models=['stg_orders'],
    link_hk_column='hk_link_order_user',
    link_key_columns=['order_id', 'user_id'],
    hub_key_map={
        'hk_order': ['order_id'],
        'hk_user': ['user_id']
    }
) }}
