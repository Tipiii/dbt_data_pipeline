{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['hk_order', 'hashdiff'],
    on_schema_change='sync_all_columns'
) }}

{{ build_sat(
    source_models=['stg_orders'],
    parent_hk_column='hk_order',
    parent_key_columns=['order_id'],
    hashdiff_columns_list=[
        'eval_set',
        'order_number',
        'order_dow',
        'order_hour_of_day',
        'days_since_prior_order'
    ],
    payload_columns=[
        'eval_set',
        'order_number',
        'order_dow',
        'order_hour_of_day',
        'days_since_prior_order'
    ]
) }}
