{{ config(
    materialized = 'view',

)}}


{% set source_model = 'order_products_prior' %}
{% set source_name = 'raw_instacart' %}
{% set business_key_cols = ['order_id', 'product_id'] %}

{{ stage(
    source_model,
    source_name,
    business_key_cols
) }}
