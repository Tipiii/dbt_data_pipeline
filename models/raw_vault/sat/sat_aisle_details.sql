{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['hk_aisle', 'hashdiff'],
    on_schema_change='sync_all_columns'
) }}

{{ build_sat(
    source_models=['stg_aisles'],
    parent_hk_column='hk_aisle',
    parent_key_columns=['aisle_id'],
    hashdiff_columns_list=['aisle'],
    payload_columns=['aisle']
) }}
