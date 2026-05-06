{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='hk_department'
) }}

{% set source_models = ['stg_departments'] %}
{% set hk_column = 'hk_department' %}
{% set business_key_columns = ['department_id'] %}

{{ build_hub(
    source_models=source_models,
    hk_column=hk_column,
    business_key_columns=business_key_columns
) }}
