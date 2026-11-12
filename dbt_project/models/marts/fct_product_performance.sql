{{ config(materialized='table') }}

-- Grain: one product x one month (zeros included).

with sales as (
    select *
    from {{ ref('int_sales_enriched') }}
    where order_date is not null
      and order_date >= cast('{{ var("min_valid_order_date") }}' as date)
),

anchor as (
    select
        cast(date_trunc('month', min(order_date)) as date) as first_month,
        cast(date_trunc('month', max(order_date)) as date) as last_month,
        max(order_date)                                    as as_of_date
    from sales
),

product_ids as (
    select product_id from {{ ref('stg_products') }} where product_id is not null
    union
    select product_id from sales where product_id is not null      -- keeps orphan sales
),

months as (
    select cast(unnest(generate_series(first_month, last_month, interval 1 month)) as date) as order_month
    from anchor
),

monthly_sales as (
    select
        product_id,
        cast(date_trunc('month', order_date) as date)                         as order_month,

        count(distinct case when not is_cancelled then order_id end)          as order_count,
        count_if(not is_cancelled)                                            as line_count,
        count_if(is_cancelled)                                                as cancelled_line_count,
        sum(case when not is_cancelled then quantity end)                     as units_sold,

        sum(case when not is_cancelled then gross_revenue end)                as gross_revenue,
        sum(recognized_discount_amount)                                       as discount_amount,
        sum(recognized_revenue)                                               as recognized_revenue,
        sum(recognized_cogs)                                                  as cost_of_goods_sold,
        sum(recognized_cogs_at_standard)                                      as cogs_at_standard_cost,
        sum(estimated_contribution_profit)                                    as estimated_contribution_profit,
        sum(recognized_shipping_fee)                                          as shipping_fee_revenue,

        count_if(return_requested and not is_cancelled)                       as lines_with_return_requests,
        sum(case when return_requested and not is_cancelled
                 then recognized_revenue end)                                 as returned_revenue_exposure,

        count_if(delivery_variance_days is not null and not is_cancelled)     as delivery_measured_lines,
        sum(case when not is_cancelled then delivery_variance_days end)       as delivery_variance_days_total,
        count_if(coalesce(is_late_delivery, false) and not is_cancelled)      as late_lines
    from sales
    group by product_id, order_month
),

joined as (
    select
        ids.product_id,
        coalesce(p.product_name, 'Unknown Product')   as product_name,
        coalesce(p.product_category, 'Unknown')       as product_category,
        p.supplier_code,
        p.seasonality_tag,
        p.is_active,
        p.launch_date,
        p.base_price,
        p.unit_cost                                   as current_standard_cost,
        (p.product_id is null)                        as is_orphan_product,

        m.order_month,
        a.as_of_date,
        (last_day(m.order_month) <= a.as_of_date)     as is_complete_month,
        (p.launch_date is null or p.launch_date <= last_day(m.order_month)) as is_listed_in_month,
        date_diff('month', cast(date_trunc('month', p.launch_date) as date), m.order_month) as months_since_launch,

        coalesce(s.order_count, 0)                    as order_count,
        coalesce(s.line_count, 0)                     as line_count,
        coalesce(s.cancelled_line_count, 0)           as cancelled_line_count,
        coalesce(s.units_sold, 0)                     as units_sold,
        coalesce(s.gross_revenue, 0)                  as gross_revenue,
        coalesce(s.discount_amount, 0)                as discount_amount,
        coalesce(s.recognized_revenue, 0)             as recognized_revenue,
        coalesce(s.cost_of_goods_sold, 0)             as cost_of_goods_sold,
        coalesce(s.cogs_at_standard_cost, 0)          as cogs_at_standard_cost,
        coalesce(s.estimated_contribution_profit, 0)  as estimated_contribution_profit,
        coalesce(s.shipping_fee_revenue, 0)           as shipping_fee_revenue,
        coalesce(s.lines_with_return_requests, 0)     as lines_with_return_requests,
        coalesce(s.returned_revenue_exposure, 0)      as returned_revenue_exposure,
        coalesce(s.delivery_measured_lines, 0)        as delivery_measured_lines,
        coalesce(s.delivery_variance_days_total, 0)   as delivery_variance_days_total,
        coalesce(s.late_lines, 0)                     as late_lines,

        -- units sold valued at CURRENT list price (for price realisation %)
        coalesce(s.units_sold, 0) * coalesce(p.base_price, 0)                           as list_price_value,
        case when p.base_price is not null then coalesce(s.gross_revenue, 0) else 0 end as priced_gross_revenue
    from product_ids ids
    cross join months m
    cross join anchor a
    left join {{ ref('stg_products') }} p on ids.product_id = p.product_id
    left join monthly_sales s
           on ids.product_id = s.product_id and m.order_month = s.order_month
)

select
    *,
    (units_sold > 0 and not is_listed_in_month)       as is_sold_while_unlisted,
    strftime(order_month, '%Y-%m')                    as month_year_label,
    extract(year from order_month)                    as order_year,
    extract(month from order_month)                   as month_number,
    strftime(order_month, '%b')                       as month_name,
    extract(year from order_month) * 100 + extract(month from order_month) as year_month_sort
from joined