{% macro unnest_key(column_name, key_to_extract, value_type) %}
(
    select params -> 'value' ->> '{{ value_type }}'
    from jsonb_array_elements({{ column_name }}::jsonb -> '{{ column_name }}') as params
    where params ->> 'key' = '{{ key_to_extract }}'
    limit 1
)
{% endmacro %}