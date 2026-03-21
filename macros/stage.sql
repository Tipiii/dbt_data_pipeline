{% macro stage(source_model) %}


  SELECT * 
  FROM {{ source('raw_instacart', source_model) }}



{% endmacro %}