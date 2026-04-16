{% macro hash_columns(columns) -%}
  to_hex(md5(concat(
    {%- for column in columns -%}
      coalesce(cast({{ column }} as string), '^^'){% if not loop.last %}, '||', {% endif %}
    {%- endfor -%}
  )))
{%- endmacro %}

{% macro hash_column(columns, source_name=None) -%}
  {{ hash_columns(columns) }}
{%- endmacro %}

{% macro hashdiff_columns(columns) -%}
  {{ hash_columns(columns) }}
{%- endmacro %}
