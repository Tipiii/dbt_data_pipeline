{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='hk_link_product_aisle'
) }}

{% set source_models = ['stg_products'] %}
{% set link_hk_column = 'hk_link_product_aisle' %}
{% set link_key_columns = ['product_id', 'aisle_id'] %}
{% set hub_key_map = {
    'hk_product': ['product_id'],
    'hk_aisle': ['aisle_id']
} %}

{{ build_link(
    source_models=source_models,
    link_hk_column=link_hk_column,
    link_key_columns=link_key_columns,
    hub_key_map=hub_key_map
) }}
