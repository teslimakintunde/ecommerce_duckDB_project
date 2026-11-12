{{ config(materialized='table') }}

-- Grain: signup month x acquisition channel x campaign.

with customers as (
    select
        customer_id, signup_date, signup_month, acquisition_channel, acquisition_campaign_id,
        completed_order_count, units_ordered, historical_revenue,
        historical_contribution_profit, ordered_before_signup, as_of_date
    from {{ ref('fct_customer_value') }}
    where signup_date is not null     -- no signup date = cannot be placed in a cohort
),

-- value earned in fixed windows after signup, so cohorts compare at equal age
early_value as (
    select
        s.customer_id,
        sum(case when (s.order_date - c.signup_date) between 0 and 30  then s.recognized_revenue end) as revenue_first_30d,
        sum(case when (s.order_date - c.signup_date) between 0 and 90  then s.recognized_revenue end) as revenue_first_90d,
        sum(case when (s.order_date - c.signup_date) between 0 and 180 then s.recognized_revenue end) as revenue_first_180d,
        sum(case when (s.order_date - c.signup_date) between 0 and 90  then s.estimated_contribution_profit end) as profit_first_90d,
        count(distinct case when (s.order_date - c.signup_date) between 0 and 30 then s.order_id end) as orders_first_30d,
        count(distinct case when (s.order_date - c.signup_date) between 0 and 90 then s.order_id end) as orders_first_90d
    from {{ ref('int_sales_enriched') }} s
    inner join customers c on s.customer_id = c.customer_id
    where s.is_cancelled = false and s.order_date is not null
    group by s.customer_id
)

select
    c.signup_month,
    c.acquisition_channel,
    c.acquisition_campaign_id,

    count(distinct c.customer_id)                                  as acquired_customers,
    count_if(c.completed_order_count > 0)                          as customers_with_orders,
    count_if(c.completed_order_count > 1)                          as repeat_customers,
    count_if(coalesce(e.orders_first_30d, 0) > 0)                  as customers_ordered_within_30d,
    count_if(coalesce(e.orders_first_90d, 0) > 0)                  as customers_ordered_within_90d,
    count_if(coalesce(e.orders_first_90d, 0) > 1)                  as repeat_within_90d,
    count_if(coalesce(c.ordered_before_signup, false))             as customers_ordered_before_signup,

    sum(c.completed_order_count)                                   as total_orders,
    sum(c.units_ordered)                                           as total_units,
    sum(c.historical_revenue)                                      as historical_revenue,
    sum(c.historical_contribution_profit)                          as historical_contribution_profit,

    sum(coalesce(e.revenue_first_30d, 0))                          as revenue_first_30d,
    sum(coalesce(e.revenue_first_90d, 0))                          as revenue_first_90d,
    sum(coalesce(e.revenue_first_180d, 0))                         as revenue_first_180d,
    sum(coalesce(e.profit_first_90d, 0))                           as profit_first_90d,

    -- a window is valid only once the LAST day of the signup month is that many days old
    date_diff('month', c.signup_month, c.as_of_date)               as cohort_age_months,
    (c.as_of_date - last_day(c.signup_month)) >= 30                as is_30d_mature,
    (c.as_of_date - last_day(c.signup_month)) >= 90                as is_90d_mature,
    (c.as_of_date - last_day(c.signup_month)) >= 180               as is_180d_mature,

    strftime(c.signup_month, '%Y-%m')                              as signup_month_label,
    extract(year from c.signup_month)                              as signup_year,
    extract(year from c.signup_month) * 100 + extract(month from c.signup_month) as signup_month_sort
from customers c
left join early_value e on c.customer_id = e.customer_id
group by c.signup_month, c.acquisition_channel, c.acquisition_campaign_id, c.as_of_date