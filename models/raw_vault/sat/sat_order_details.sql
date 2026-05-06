{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['hk_order', 'hashdiff']
) }}

{% set source_models = ['stg_orders'] %}
{% set parent_hk_column = 'hk_order' %}
{% set parent_key_columns = ['order_id'] %}
{% set hashdiff_columns_list = [
    'eval_set',
    'order_number',
    'order_dow',
    'order_hour_of_day',
    'days_since_prior_order'
] %}
{% set payload_columns = [
    'eval_set',
    'order_number',
    'order_dow',
    'order_hour_of_day',
    'days_since_prior_order'
] %}

{{ build_sat(
    source_models=source_models,
    parent_hk_column=parent_hk_column,
    parent_key_columns=parent_key_columns,
    hashdiff_columns_list=hashdiff_columns_list,
    payload_columns=payload_columns
) }}
