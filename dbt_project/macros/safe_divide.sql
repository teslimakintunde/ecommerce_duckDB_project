{% macro safe_divide(numerator, denominator) -%}
(cast({{ numerator }} as double) / nullif({{ denominator }}, 0))
{%- endmacro %}