{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='hk_aisle'
) }}

{% set source_models = ['stg_aisles'] %}
{% set hk_column = 'hk_aisle' %}
{% set business_key_columns = ['aisle_id'] %}

{{ build_hub(
    source_models=source_models,
    hk_column=hk_column,
    business_key_columns=business_key_columns
) }}
