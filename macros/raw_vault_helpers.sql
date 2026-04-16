{% macro raw_vault_as_list(value) -%}
  {%- if value is string -%}
    {{ return([value]) }}
  {%- else -%}
    {{ return(value) }}
  {%- endif -%}
{%- endmacro %}

{% macro raw_vault_unique_columns(primary_columns, extra_columns=[]) -%}
  {% set namespace = namespace(columns=[]) %}
  {% for column in raw_vault_as_list(primary_columns) + raw_vault_as_list(extra_columns) %}
    {% if column not in namespace.columns %}
      {% do namespace.columns.append(column) %}
    {% endif %}
  {% endfor %}
  {{ return(namespace.columns) }}
{%- endmacro %}

{% macro raw_vault_not_null_condition(columns) -%}
  {%- for column in raw_vault_as_list(columns) -%}
    {{ column }} is not null{% if not loop.last %} and {% endif %}
  {%- endfor -%}
{%- endmacro %}

{% macro raw_vault_union_sources(source_models, columns) -%}
  {% set models = raw_vault_as_list(source_models) %}
  {% set select_columns = raw_vault_as_list(columns) %}
  {%- for model in models %}
    select
      {%- for column in select_columns %}
        {{ column }},
      {%- endfor %}
      load_timestamp,
      record_source
    from {{ ref(model) }}
    {% if not loop.last %}union all{% endif %}
  {%- endfor %}
{%- endmacro %}
