{% macro parse_mixed_date(column_expression) %}
  {#- ambiguous_date_order: 'mdy' (default) | 'dmy' | 'null' -#}
  {%- set amb = var('ambiguous_date_order', 'mdy') -%}
  {%- set s = "regexp_replace(regexp_replace(trim(cast(" ~ column_expression ~ " as varchar)), '(?i)-Sept-', '-Sep-'), '[ T][0-9]{1,2}:[0-9]{2}.*$', '')" -%}
  cast(
    case
      when nullif({{ s }}, '') is null then null

      -- 2024-06-09
      when regexp_matches({{ s }}, '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$')
        then try_strptime({{ s }}, '%Y-%m-%d')

      -- 09-Jun-2024
      when regexp_matches({{ s }}, '^[0-9]{1,2}-[A-Za-z]{3}-[0-9]{4}$')
        then try_strptime({{ s }}, '%d-%b-%Y')

      -- 09-Jun-24
      when regexp_matches({{ s }}, '^[0-9]{1,2}-[A-Za-z]{3}-[0-9]{2}$')
        then try_strptime({{ s }}, '%d-%b-%y')

      -- 15/06/2024 or 6/15/2024 or ambiguous 1/5/2024
      when regexp_matches({{ s }}, '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$') then
        case
          when try_cast(split_part({{ s }}, '/', 1) as integer) > 12
            then try_strptime({{ s }}, '%d/%m/%Y')
          when try_cast(split_part({{ s }}, '/', 2) as integer) > 12
            then try_strptime({{ s }}, '%m/%d/%Y')
          {% if amb == 'dmy' -%}
          else try_strptime({{ s }}, '%d/%m/%Y')
          {%- elif amb == 'null' -%}
          else null
          {%- else -%}
          else try_strptime({{ s }}, '%m/%d/%Y')
          {%- endif %}
        end

      else null
    end
  as date)
{% endmacro %}