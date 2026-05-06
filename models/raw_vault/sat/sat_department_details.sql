{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['hk_department', 'hashdiff']
) }}

{% set source_models = ['stg_departments'] %}
{% set parent_hk_column = 'hk_department' %}
{% set parent_key_columns = ['department_id'] %}
{% set hashdiff_columns_list = ['department'] %}
{% set payload_columns = ['department'] %}

{{ build_sat(
    source_models=source_models,
    parent_hk_column=parent_hk_column,
    parent_key_columns=parent_key_columns,
    hashdiff_columns_list=hashdiff_columns_list,
    payload_columns=payload_columns
) }}
