{{ config(
    materialized = 'view',

)}}

SELECT 
    aisle_id,
    aisle
FROM {{ source('raw_instacart', 'aisles') }}

{% set source_model = 'aisles' %}
{% set source_name = 'raw_instacart' %}

{{ stage(
    source_model, 
    source_name
) }}
