{{ config(materialized='table') }}

-- Grain: one customer.

with customer_base as (
    select
        customer_id, gender, city, customer_region, signup_date,
        cast(date_trunc('month', signup_date) as date) as signup_month,
        invalid_signup_date, loyalty_tier, preferred_payment, customer_age,
        case
          when customer_age is null                    then 'Unknown'
          when customer_age < 16 or customer_age > 100 then 'Invalid'
          when customer_age < 25                       then 'Under 25'
          when customer_age < 35                       then '25-34'
          when customer_age < 45                       then '35-44'
          when customer_age < 55                       then '45-54'
          else '55+'
        end as age_band,
        customer_segment,
        acquisition_channel,
        coalesce(acquisition_campaign_id, 'Unknown') as acquisition_campaign_id
    from {{ ref('stg_customers') }}
),

-- latest completed order in the data = "today" for recency and tenure
anchor as (
    select max(order_date) as as_of_date
    from {{ ref('int_sales_enriched') }}
    where is_cancelled = false and order_date is not null
),

customer_sales as (
    select
        customer_id,
        count(distinct order_id)                                      as total_order_count,
        count(distinct case when not is_cancelled then order_id end)  as completed_order_count,
        count_if(is_cancelled)                                        as cancelled_line_count,
        count_if(not is_cancelled)                                    as completed_line_count,

        sum(case when not is_cancelled then gross_revenue end)        as historical_gross_revenue,
        sum(recognized_discount_amount)                               as historical_discount_amount,
        sum(recognized_revenue)                                       as historical_revenue,
        sum(estimated_contribution_profit)                            as historical_contribution_profit,
        sum(case when not is_cancelled then quantity end)             as units_ordered,

        min(case when not is_cancelled then order_date end)           as first_order_date,
        max(case when not is_cancelled then order_date end)           as last_order_date,

        count_if(return_requested and not is_cancelled)               as lines_with_return_requests,
        sum(case when return_requested and not is_cancelled
                 then recognized_revenue end)                         as returned_revenue_exposure
    from {{ ref('int_sales_enriched') }}
    where customer_id is not null
    group by customer_id
),

joined as (
    select
        c.*,
        a.as_of_date,
        coalesce(s.total_order_count, 0)              as total_order_count,
        coalesce(s.completed_order_count, 0)          as completed_order_count,
        coalesce(s.cancelled_line_count, 0)           as cancelled_line_count,
        coalesce(s.completed_line_count, 0)           as completed_line_count,
        coalesce(s.historical_gross_revenue, 0)       as historical_gross_revenue,
        coalesce(s.historical_discount_amount, 0)     as historical_discount_amount,
        coalesce(s.historical_revenue, 0)             as historical_revenue,
        coalesce(s.historical_contribution_profit, 0) as historical_contribution_profit,
        coalesce(s.units_ordered, 0)                  as units_ordered,
        s.first_order_date,
        s.last_order_date,
        coalesce(s.lines_with_return_requests, 0)     as lines_with_return_requests,
        coalesce(s.returned_revenue_exposure, 0)      as returned_revenue_exposure,
        (a.as_of_date - s.last_order_date)            as recency_days,
        (a.as_of_date - c.signup_date)                as tenure_days
    from customer_base c
    left join customer_sales s on c.customer_id = s.customer_id
    cross join anchor a
),

-- value ranking only among customers who actually bought
scored as (
    select
        *,
        case when completed_order_count > 0
             then ntile(10) over (partition by (completed_order_count > 0)
                                  order by historical_revenue desc, customer_id)
        end as revenue_decile,
        case when completed_order_count > 0
             then percent_rank() over (partition by (completed_order_count > 0)
                                       order by historical_revenue desc, customer_id)
        end as revenue_percent_rank
    from joined
)

select
    *,
    {{ safe_divide('historical_revenue', 'completed_order_count') }}           as historical_average_order_value,
    {{ safe_divide('historical_contribution_profit', 'historical_revenue') }}  as historical_profit_margin,
    {{ safe_divide('lines_with_return_requests', 'completed_line_count') }}    as customer_return_rate,

    (first_order_date - signup_date)                                           as days_signup_to_first_order,
    (last_order_date - first_order_date)                                       as active_span_days,
    {{ safe_divide('(last_order_date - first_order_date)', '(completed_order_count - 1)') }} as avg_days_between_orders,

    (first_order_date < signup_date)                                           as ordered_before_signup,
    (total_order_count > 0 and completed_order_count = 0)                      as has_only_cancelled_orders,

    case when completed_order_count = 0 then 'No Recorded Orders'
         when completed_order_count = 1 then 'One-Time Customer'
         else 'Repeat Customer' end                                            as purchase_frequency_segment,
    case when completed_order_count = 0 then 1
         when completed_order_count = 1 then 2 else 3 end                      as purchase_frequency_sort,

    case when completed_order_count = 0 then 'Never Purchased'
         when recency_days <= 30  then 'Active (0-30 Days)'
         when recency_days <= 90  then 'Lapsing (31-90 Days)'
         when recency_days <= 180 then 'Lapsed (91-180 Days)'
         else 'Churned (181+ Days)' end                                        as recency_segment,
    case when completed_order_count = 0 then 5
         when recency_days <= 30  then 1
         when recency_days <= 90  then 2
         when recency_days <= 180 then 3 else 4 end                            as recency_segment_sort,

    case when completed_order_count = 0     then 'No Orders'
         when revenue_percent_rank <= 0.10  then 'Top 10%'
         when revenue_percent_rank <= 0.30  then 'Next 20%'
         else 'Bottom 70%' end                                                 as value_tier,
    case when completed_order_count = 0     then 4
         when revenue_percent_rank <= 0.10  then 1
         when revenue_percent_rank <= 0.30  then 2 else 3 end                  as value_tier_sort
from scored