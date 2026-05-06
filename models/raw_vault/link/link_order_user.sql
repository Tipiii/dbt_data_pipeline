{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='hk_link_order_user'
) }}

{% set source_models = ['stg_orders'] %}
{% set link_hk_column = 'hk_link_order_user' %}
{% set link_key_columns = ['order_id', 'user_id'] %}
{% set hub_key_map = {
    'hk_order': ['order_id'],
    'hk_user': ['user_id']
} %}

{{ build_link(
    source_models=source_models,
    link_hk_column=link_hk_column,
    link_key_columns=link_key_columns,
    hub_key_map=hub_key_map
) }}
