{% macro stage(source_model, source_name, business_key_cols=[]) %}

  select
    *,
  {% if business_key_cols | length > 0 %}
    {{ hash_columns(business_key_cols) }} as hashkey,
  {% endif %}
    parse_date('%Y%m%d', '{{ var("target_date", run_started_at.strftime("%Y%m%d")) }}') as source_event_date,
    concat('{{ source_name }}', '_', '{{ source_model }}') as record_source,
    current_timestamp() as load_timestamp
  from {{ source(source_name, source_model) }}

{% endmacro %}
