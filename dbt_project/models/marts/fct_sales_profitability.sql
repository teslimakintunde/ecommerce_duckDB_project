{{ config(
    materialized='incremental',
    unique_key='line_item_id',
    incremental_strategy='delete+insert',
    on_schema_change='append_new_columns'
) }}

-- Grain: one sales line.

select
    line_item_id, order_id, customer_id, product_id,

    product_name, product_category, supplier_code,
    loyalty_tier, customer_segment, acquisition_channel,

    region, sales_channel, payment_method, campaign_id, warehouse_id,
    order_priority, delivery_status,

    quantity, unit_price, unit_cost, standard_unit_cost,
    discount_applied, discount_band, discount_band_sort,

    gross_revenue, discount_amount, sales_after_discount,
    cost_of_goods_sold, shipping_fee_amount,

    recognized_revenue, recognized_discount_amount, recognized_cogs,
    recognized_cogs_at_standard, recognized_shipping_fee,
    estimated_contribution_profit,
    estimated_contribution_profit as gross_profit,

    is_cancelled, return_requested, is_free_shipping,
    has_missing_cost, has_missing_standard_cost,

    order_date,
    cast(date_trunc('month', order_date) as date)          as order_month,
    cast(date_trunc('quarter', order_date) as date)        as order_quarter,
    extract(year from order_date)                          as order_year,
    extract(month from order_date)                         as month_number,
    strftime(order_date, '%b')                             as month_name,
    strftime(order_date, '%B')                             as month_name_full,
    strftime(order_date, '%Y-%m')                          as month_year_label,
    extract(year from order_date) * 100 + extract(month from order_date) as year_month_sort,

    record_updated_at,
    dbt_loaded_at
from {{ ref('int_sales_enriched') }}
{{ incremental_filter('record_updated_at') }}