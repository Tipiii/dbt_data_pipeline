{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['hk_product', 'hashdiff']
) }}

{% set source_models = ['stg_products'] %}
{% set parent_hk_column = 'hk_product' %}
{% set parent_key_columns = ['product_id'] %}
{% set hashdiff_columns_list = ['product_name'] %}
{% set payload_columns = ['product_name'] %}

{{ build_sat(
    source_models=source_models,
    parent_hk_column=parent_hk_column,
    parent_key_columns=parent_key_columns,
    hashdiff_columns_list=hashdiff_columns_list,
    payload_columns=payload_columns
) }}
