{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='hk_user',
    tags=['raw_vault', 'hub', 'hub_user']
) }}

{% set source_models = ['stg_orders'] %}
{% set hk_column = 'hk_user' %}
{% set business_key_columns = ['user_id'] %}

{{ build_hub(
    source_models=source_models,
    hk_column=hk_column,
    business_key_columns=business_key_columns
) }}
