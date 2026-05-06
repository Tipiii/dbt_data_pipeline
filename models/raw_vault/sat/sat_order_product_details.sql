{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['hk_link_order_product', 'hashdiff']
) }}

{% set source_models = ['stg_order_products_prior', 'stg_order_products_train'] %}
{% set parent_hk_column = 'hk_link_order_product' %}
{% set parent_key_columns = ['order_id', 'product_id'] %}
{% set hashdiff_columns_list = ['add_to_cart_order', 'reordered'] %}
{% set payload_columns = ['add_to_cart_order', 'reordered'] %}

{{ build_sat(
    source_models=source_models,
    parent_hk_column=parent_hk_column,
    parent_key_columns=parent_key_columns,
    hashdiff_columns_list=hashdiff_columns_list,
    payload_columns=payload_columns
) }}
