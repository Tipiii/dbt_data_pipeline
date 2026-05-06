{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['hk_aisle', 'hashdiff']
) }}

{% set source_models = ['stg_aisles'] %}
{% set parent_hk_column = 'hk_aisle' %}
{% set parent_key_columns = ['aisle_id'] %}
{% set hashdiff_columns_list = ['aisle'] %}
{% set payload_columns = ['aisle'] %}

{{ build_sat(
    source_models=source_models,
    parent_hk_column=parent_hk_column,
    parent_key_columns=parent_key_columns,
    hashdiff_columns_list=hashdiff_columns_list,
    payload_columns=payload_columns
) }}
