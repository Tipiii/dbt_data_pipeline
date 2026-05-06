{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='hk_product'
) }}

{% set source_models = ['stg_products'] %}
{% set hk_column = 'hk_product' %}
{% set business_key_columns = ['product_id'] %}

{{ build_hub(
    source_models=source_models,
    hk_column=hk_column,
    business_key_columns=business_key_columns
) }}
