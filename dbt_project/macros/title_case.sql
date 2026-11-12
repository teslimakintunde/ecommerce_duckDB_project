{% macro title_case(col) -%}
list_aggr(
    list_transform(
        string_split(lower(trim(cast({{ col }} as varchar))), ' '),
        w -> upper(left(w, 1)) || substr(w, 2)
    ),
    'string_agg', ' '
)
{%- endmacro %}