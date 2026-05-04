{% macro build_sat(source_models, parent_hk_column, parent_key_columns, hashdiff_columns_list, payload_columns) -%}

{% set source_columns = raw_vault_unique_columns(parent_key_columns, payload_columns + ['source_event_date']) %}

with source_data as (
    {{ raw_vault_union_sources(source_models, source_columns) }}
),
hashed as (
    select
        {{ hash_columns(parent_key_columns) }} as {{ parent_hk_column }},
        {{ hashdiff_columns(hashdiff_columns_list) }} as hashdiff,
        {%- for column in raw_vault_as_list(payload_columns) %}
        {{ column }},
        {%- endfor %}
        source_event_date as effective_from,
        load_timestamp,
        record_source
    from source_data
    where {{ raw_vault_not_null_condition(parent_key_columns) }}
),
deduped as (
    select *
    from hashed
    qualify row_number() over (
        partition by {{ parent_hk_column }}, hashdiff
        order by load_timestamp, record_source
    ) = 1
),
incremental_rows as (
    select *
    from deduped
    {{ raw_vault_incremental_not_exists('deduped', [parent_hk_column, 'hashdiff']) }}
)
select *
from incremental_rows
{%- endmacro %}
