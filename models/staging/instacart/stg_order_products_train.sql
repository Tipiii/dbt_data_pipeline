{{ config(
    materialized = 'view',

)}}


{% set source_table = 'order_products_train' %}
{% set source_name = 'raw_instacart' %}
{% set business_key_cols = ['order_id', 'product_id'] %}
{% set hashdiff_satellite_dict = none %}

{{ stage(
    source_model=source_table,
    source_name=source_name,
    business_key_cols=business_key_cols
) }}
