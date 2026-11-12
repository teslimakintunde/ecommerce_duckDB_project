{{ config(
    materialized='incremental',
    unique_key='line_item_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- Grain: one sales line.
-- Incremental: only lines changed since the last run (by updated_at), plus any
-- line whose product/customer was missing last time (late-arriving dimensions).

with sales as (
    select * from {{ ref('stg_sales') }}
    {% if is_incremental() %}
    where record_updated_at >= (
            select coalesce(max(record_updated_at), timestamp '1900-01-01')
                   - interval '{{ var("incremental_lag_hours") }} hours'
            from {{ this }})
       or line_item_id in (
            select line_item_id from {{ this }}
            where is_orphan_product or is_orphan_customer)
    {% endif %}
),

products  as (select * from {{ ref('stg_products') }}),
customers as (select * from {{ ref('stg_customers') }}),

joined as (
    select
        s.line_item_id, s.order_id, s.customer_id, s.product_id,
        s.order_date, s.estimated_delivery, s.actual_delivery,
        s.delivery_status, s.region, s.sales_channel, s.payment_method,
        s.campaign_id, s.warehouse_id, s.order_priority, s.shipping_city,
        s.quantity, s.unit_price, s.unit_cost, s.discount_applied, s.shipping_cost,
        s.return_requested, s.return_reason, s.cancellation_reason,
        s.created_at, s.updated_at, s.record_updated_at,

        p.product_name, p.product_category, p.supplier_code, p.country_of_origin,
        p.seasonality_tag, p.is_active as product_is_active,
        p.launch_date as product_launch_date,
        p.unit_cost   as standard_unit_cost,     -- CURRENT standard cost, not historical

        c.gender, c.loyalty_tier, c.customer_segment, c.acquisition_channel,
        c.acquisition_campaign_id, c.signup_date as customer_signup_date,

        (p.product_id is null) as is_orphan_product,
        (c.customer_id is null) as is_orphan_customer
    from sales s
    left join products  p on s.product_id  = p.product_id
    left join customers c on s.customer_id = c.customer_id
),

calculated as (
    select
        *,
        coalesce(lower(delivery_status) = 'cancelled' or cancellation_reason is not null, false)
                                                                          as is_cancelled_flag,
        quantity * unit_price                                             as gross_revenue,
        quantity * unit_price * coalesce(discount_applied, 0)             as discount_amount,
        quantity * unit_price * (1 - coalesce(discount_applied, 0))       as sales_after_discount,

        coalesce(quantity * unit_cost, 0)                                 as cost_of_goods_sold,
        coalesce(quantity * standard_unit_cost, 0)                        as cogs_at_standard_cost,
        (unit_cost is null)                                               as has_missing_cost,
        (standard_unit_cost is null)                                      as has_missing_standard_cost,

        coalesce(shipping_cost, 0)                                        as shipping_fee_amount,
        (coalesce(shipping_cost, 0) = 0)                                  as is_free_shipping,

        (actual_delivery - estimated_delivery)                            as delivery_variance_days,
        (actual_delivery > estimated_delivery)                            as is_late_delivery,

        coalesce(return_requested, false)                                 as return_requested_clean,

        -- data-quality flags
        (actual_delivery < order_date or estimated_delivery < order_date) as has_date_sequence_issue,
        (delivery_status = 'Delivered' and actual_delivery is null)       as has_status_date_conflict,
        (coalesce(return_requested, false) and return_reason is null)     as has_return_reason_missing
    from joined
)

select
    line_item_id, order_id, customer_id, product_id,

    product_name, product_category, supplier_code, country_of_origin,
    seasonality_tag, product_is_active,

    gender, loyalty_tier, customer_segment, acquisition_channel,
    acquisition_campaign_id, customer_signup_date,

    region, sales_channel, payment_method, campaign_id, warehouse_id,
    order_priority, shipping_city, delivery_status, cancellation_reason, return_reason,
    return_requested_clean as return_requested,

    order_date, estimated_delivery, actual_delivery, delivery_variance_days, is_late_delivery,

    quantity, unit_price, unit_cost, standard_unit_cost, discount_applied,
    has_missing_cost, has_missing_standard_cost,

    gross_revenue, discount_amount, sales_after_discount,
    cost_of_goods_sold, cogs_at_standard_cost, shipping_fee_amount, is_free_shipping,

    is_cancelled_flag as is_cancelled,

    case when is_cancelled_flag then 0 else sales_after_discount end   as recognized_revenue,
    case when is_cancelled_flag then 0 else discount_amount end        as recognized_discount_amount,
    case when is_cancelled_flag then 0 else cost_of_goods_sold end     as recognized_cogs,
    case when is_cancelled_flag then 0 else cogs_at_standard_cost end  as recognized_cogs_at_standard,
    case when is_cancelled_flag then 0 else shipping_fee_amount end    as recognized_shipping_fee,
    case when is_cancelled_flag then 0
         else sales_after_discount - cost_of_goods_sold end            as estimated_contribution_profit,

    case
      when discount_applied is null                     then 'Unknown'
      when discount_applied < 0 or discount_applied > 1 then 'Invalid Discount'
      when discount_applied = 0                         then 'No Discount'
      when discount_applied <= 0.05                     then 'Low (<=5%)'
      when discount_applied <= 0.10                     then 'Moderate (5-10%)'
      when discount_applied <= 0.15                     then 'High (10-15%)'
      else 'Very High (>15%)'
    end as discount_band,
    case
      when discount_applied is null                     then 7
      when discount_applied < 0 or discount_applied > 1 then 6
      when discount_applied = 0                         then 1
      when discount_applied <= 0.05                     then 2
      when discount_applied <= 0.10                     then 3
      when discount_applied <= 0.15                     then 4
      else 5
    end as discount_band_sort,

    is_orphan_product, is_orphan_customer,
    has_date_sequence_issue, has_status_date_conflict, has_return_reason_missing,

    created_at, updated_at, record_updated_at,
    cast(current_timestamp as timestamp) as dbt_loaded_at
from calculated