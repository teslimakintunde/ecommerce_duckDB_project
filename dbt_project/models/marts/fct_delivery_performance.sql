{{ config(materialized='table') }}

-- Grain: one order (lines rolled up).

with lines as (
    select * from {{ ref('int_sales_enriched') }}
),

anchor as (
    select greatest(max(order_date), max(actual_delivery)) as as_of_date from lines
),

order_rollup as (
    select
        order_id,
        max(customer_id)                              as customer_id,
        max(region)                                   as region,
        max(sales_channel)                            as sales_channel,

        case when count(distinct warehouse_id) > 1
             then string_agg(distinct warehouse_id, ', ')
             else max(warehouse_id) end               as warehouse_id,
        count(distinct warehouse_id) > 1              as has_multiple_warehouses,
        count(distinct region) > 1                    as has_multiple_regions,
        max(order_priority)                           as order_priority,
        min(order_date)                               as order_date,

        -- promise and arrival use ACTIVE (non-cancelled) lines only
        max(case when not is_cancelled then estimated_delivery end)     as estimated_delivery,
        max(case when not is_cancelled then actual_delivery end)        as last_actual_delivery,
        max(case when not is_cancelled then delivery_variance_days end) as worst_line_variance_days,

        count(*)                                      as line_item_count,
        count_if(not is_cancelled)                    as active_line_count,
        sum(quantity)                                 as total_units,
        sum(case when not is_cancelled then quantity end) as active_units,

        bool_or(is_cancelled)                         as has_cancelled_line,
        bool_and(is_cancelled)                        as all_lines_cancelled,
        bool_or(not is_cancelled and lower(delivery_status) = 'delayed')   as has_delayed_line,
        bool_or(not is_cancelled and coalesce(is_late_delivery, false))    as has_late_line,

        -- delivered = every active line has an actual date, and at least one active line exists
        (bool_and(is_cancelled or actual_delivery is not null)
          and not bool_and(is_cancelled))             as all_active_lines_delivered
    from lines
    group by order_id
),

final as (
    select
        r.*,
        a.as_of_date,
        case when r.all_active_lines_delivered then r.last_actual_delivery end as actual_delivery
    from order_rollup r
    cross join anchor a
)

select
    order_id, customer_id, region, sales_channel, warehouse_id,
    has_multiple_warehouses, has_multiple_regions, order_priority,
    order_date, estimated_delivery, actual_delivery, as_of_date,
    line_item_count, active_line_count, total_units, active_units,
    has_cancelled_line, all_lines_cancelled, has_delayed_line, has_late_line,
    all_active_lines_delivered,

    case
      when all_lines_cancelled                          then 'Cancelled'
      when all_active_lines_delivered and has_late_line then 'Delivered Late'
      when all_active_lines_delivered                   then 'Delivered'
      when last_actual_delivery is not null             then 'Partially Delivered'
      when has_delayed_line                             then 'Delayed (Not Yet Delivered)'
      else 'In Progress'
    end as order_delivery_status,

    case when all_active_lines_delivered
         then (actual_delivery - order_date) end            as delivery_days,

    -- positive = at least one line arrived after its promise
    case when all_active_lines_delivered
         then worst_line_variance_days end                  as delivery_variance_days,

    case
      when all_lines_cancelled or not all_active_lines_delivered or estimated_delivery is null then null
      else has_late_line
    end as is_late_order,

    case
      when all_lines_cancelled                                       then 'Cancelled'
      when all_active_lines_delivered and estimated_delivery is null then 'No SLA Data'
      when all_active_lines_delivered and has_late_line              then 'Late'
      when all_active_lines_delivered                                then 'On Time'
      when estimated_delivery is not null
           and as_of_date > estimated_delivery                       then 'Overdue (Undelivered)'
      else 'In Progress'
    end as delivery_sla_result,

    case
      when not all_lines_cancelled and not all_active_lines_delivered
           and estimated_delivery is not null and as_of_date > estimated_delivery
      then (as_of_date - estimated_delivery)
    end as days_overdue,

    -- right-censoring guard: orders younger than 21 days may not have arrived yet
    (order_date <= as_of_date - 21)                          as is_sla_mature,

    cast(date_trunc('month', order_date) as date)            as order_month,
    strftime(order_date, '%Y-%m')                            as month_year_label,
    extract(year from order_date)                            as order_year,
    extract(year from order_date) * 100 + extract(month from order_date) as year_month_sort
from final