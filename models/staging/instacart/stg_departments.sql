{{ config(
    materialized = 'view',

)}}


{% set source_model = 'departments' %}
{% set source_name = 'raw_instacart' %}
{% set business_key_cols = ['department_id'] %}


{{ stage(
    source_model, 
    source_name,
    business_key_cols
) }}
