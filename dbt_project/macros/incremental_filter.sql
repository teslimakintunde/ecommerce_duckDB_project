{% macro incremental_filter(ts_col) -%}
{% if is_incremental() %}
where {{ ts_col }} >= (
    select coalesce(max({{ ts_col }}), timestamp '1900-01-01')
           - interval '{{ var("incremental_lag_hours") }} hours'
    from {{ this }}
)
{% endif %}
{%- endmacro %}