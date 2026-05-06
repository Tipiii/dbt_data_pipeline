{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='hk_order'
) }}

{% set source_models = ['stg_orders'] %}
{% set hk_column = 'hk_order' %}
{% set business_key_columns = ['order_id'] %}

{{ build_hub(
    source_models=source_models,
    hk_column=hk_column,
    business_key_columns=business_key_columns
) }}
