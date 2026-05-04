{% macro build_link(source_models, link_hk_column, link_key_columns, hub_key_map) -%}

{% set namespace = namespace(hub_source_columns=[]) %}
{% for _, source_columns in hub_key_map.items() %}
  {% for column in raw_vault_as_list(source_columns) %}
    {% if column not in namespace.hub_source_columns %}
      {% do namespace.hub_source_columns.append(column) %}
    {% endif %}
  {% endfor %}
{% endfor %}
{% set source_columns = raw_vault_unique_columns(link_key_columns, namespace.hub_source_columns) %}

with source_data as (
    {{ raw_vault_union_sources(source_models, source_columns) }}
),
hashed as (
    select
        {{ hash_columns(link_key_columns) }} as {{ link_hk_column }},
        {%- for hk_name, source_columns in hub_key_map.items() %}
        {{ hash_columns(raw_vault_as_list(source_columns)) }} as {{ hk_name }},
        {%- endfor %}
        load_timestamp,
        record_source
    from source_data
    where {{ raw_vault_not_null_condition(link_key_columns) }}
),
deduped as (
    select *
    from hashed
    qualify row_number() over (
        partition by {{ link_hk_column }}
        order by load_timestamp, record_source
    ) = 1
),
incremental_rows as (
    select *
    from deduped
    {{ raw_vault_incremental_not_exists('deduped', [link_hk_column]) }}
)
select *
from incremental_rows
{%- endmacro %}
