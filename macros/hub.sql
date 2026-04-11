{% macro build_hub(source_models, hk_column, business_key_columns) -%}
with source_data as (
    select
        {{ hash_columns(business_key_columns) }} as {{ hk_column }},
        {%- for column in raw_vault_as_list(business_key_columns) %}
        {{ column }},
        {%- endfor %}
        load_timestamp,
        record_source
    from (
        {{ raw_vault_union_sources(source_models, business_key_columns) }}
    )
    where {{ raw_vault_not_null_condition(business_key_columns) }}
),
deduped as (
    select *
    from source_data
    qualify row_number() over (
        partition by {{ hk_column }}
        order by load_timestamp, record_source
    ) = 1
)
select *
from deduped
{%- endmacro %}
