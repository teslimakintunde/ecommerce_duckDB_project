{% macro clean_text(col) -%}
nullif(trim(cast({{ col }} as varchar)), '')
{%- endmacro %}