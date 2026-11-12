{{ config(materialized='table') }}

-- Grain: one sales line.
-- Change threshold: dbt build --vars '{high_discount_threshold: 0.12}'
{% set high_discount = var('high_discount_threshold') %}

with base as (
    select
        *,
        coalesce(return_requested, false)                        as raw_is_returned,
        coalesce(is_late_delivery, false)                        as raw_is_late,
        (is_late_delivery is not null)                           as raw_is_delivery_measured,
        (coalesce(discount_applied, 0) >= {{ high_discount }})   as raw_is_high_discount
    from {{ ref('int_sales_enriched') }}
),

flags as (
    select
        *,
        (raw_is_returned and not is_cancelled)                   as is_returned,
        (raw_is_late and not is_cancelled)                       as is_late,
        (raw_is_delivery_measured and not is_cancelled)          as is_delivery_measured,
        (raw_is_high_discount and not is_cancelled)              as is_high_discount,
        (is_cancelled and (raw_is_returned or raw_is_late or raw_is_high_discount))
                                                                 as cancelled_with_other_flags
    from base
)

select
    line_item_id, order_id, customer_id, product_id,
    product_name, product_category, supplier_code,

    region, sales_channel, payment_method, campaign_id, warehouse_id, order_priority,

    delivery_status, estimated_delivery, actual_delivery, delivery_variance_days,
    discount_applied, discount_band, discount_band_sort,

    is_cancelled, is_returned, is_late, is_delivery_measured, is_high_discount,
    raw_is_returned, raw_is_late, raw_is_high_discount, cancelled_with_other_flags,

    case when is_returned
         then coalesce({{ title_case('return_reason') }}, 'Not Provided')
         else 'No Return' end                                    as return_reason,
    case when is_cancelled
         then coalesce({{ title_case('cancellation_reason') }}, 'Not Provided')
         else 'Not Cancelled' end                                as cancellation_reason,

    gross_revenue, discount_amount, sales_after_discount,
    recognized_revenue, recognized_cogs, estimated_contribution_profit,

    case when is_cancelled then gross_revenue else 0 end                                   as cancelled_gross_revenue,
    -- un-gated cost on purpose (recognized_cogs is 0 for cancelled lines)
    case when is_cancelled then sales_after_discount - cost_of_goods_sold else 0 end       as cancelled_potential_profit,
    recognized_discount_amount,
    case when is_high_discount then discount_amount else 0 end                             as high_discount_amount,

    -- exposure: subsets of recognized revenue, not additional loss
    case when is_returned then recognized_revenue else 0 end                               as returned_revenue,
    case when is_returned then estimated_contribution_profit else 0 end                    as returned_profit,
    case when is_late then recognized_revenue else 0 end                                   as late_revenue,

    cast(is_cancelled as bigint)               as is_cancelled_metric,
    cast(is_returned as bigint)                as is_returned_metric,
    cast(is_late as bigint)                    as is_late_metric,
    cast(is_delivery_measured as bigint)       as is_delivery_measured_metric,
    cast(is_high_discount as bigint)           as is_heavily_discounted_metric,
    cast(cancelled_with_other_flags as bigint) as is_cancelled_with_other_flags_metric,

    cast(is_cancelled as bigint) + cast(is_returned as bigint)
      + cast(is_late as bigint) + cast(is_high_discount as bigint)                         as risk_flag_count,
    (is_cancelled or is_returned or is_late or is_high_discount)                           as is_any_risk_flag,

    case
      when is_cancelled then 'Cancelled'
      when cast(is_returned as bigint) + cast(is_late as bigint)
           + cast(is_high_discount as bigint) >= 2 then 'Multiple Flags'
      when is_returned then 'Returned'
      when is_late then 'Late Delivery'
      when is_high_discount then 'High Discount'
      else 'No Flag'
    end as risk_profile,
    case
      when is_cancelled then 1
      when cast(is_returned as bigint) + cast(is_late as bigint)
           + cast(is_high_discount as bigint) >= 2 then 2
      when is_returned then 3
      when is_late then 4
      when is_high_discount then 5
      else 6
    end as risk_profile_sort,

    case when not is_cancelled and (is_returned or is_late or is_high_discount)
         then recognized_revenue else 0 end                      as flagged_recognized_revenue,

    order_date,
    cast(date_trunc('month', order_date) as date)                as order_month,
    extract(year from order_date)                                as order_year,
    strftime(order_date, '%Y-%m')                                as month_year_label,
    extract(year from order_date) * 100 + extract(month from order_date) as year_month_sort
from flags