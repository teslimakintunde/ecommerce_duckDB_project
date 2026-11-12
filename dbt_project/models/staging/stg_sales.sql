{{ config(materialized='view') }}

with source_sales as (
    select * from {{ source('raw', 'sales_data_raw') }}
)

select
    {{ clean_text('order_id') }}      as order_id,
    {{ clean_text('line_item_id') }}  as line_item_id,
    {{ clean_text('customer_id') }}   as customer_id,
    {{ clean_text('product_id') }}    as product_id,

    try_cast(quantity as integer)             as quantity,
    try_cast(unit_price as decimal(12,2))     as unit_price,
    try_cast(unit_cost as decimal(12,2))      as unit_cost,
    try_cast(discount_applied as double)      as discount_applied,   -- fraction 0-1
    try_cast(shipping_cost as decimal(12,2))  as shipping_cost,

    {{ parse_mixed_date('order_date') }}          as order_date,
    {{ parse_mixed_date('estimated_delivery') }}  as estimated_delivery,
    {{ parse_mixed_date('actual_delivery') }}     as actual_delivery,

    case
      when lower(trim(delivery_status)) in ('delivered','deliverd','delivred','deliverred') then 'Delivered'
      when lower(trim(delivery_status)) in ('delayed','delayyed','delayd','delaied')        then 'Delayed'
      when lower(trim(delivery_status)) in ('cancelled','canceled','cancled')               then 'Cancelled'
      when lower(trim(delivery_status)) = 'pending'                                         then 'Pending'
      when lower(trim(delivery_status)) = 'processing'                                      then 'Processing'
      when {{ clean_text('delivery_status') }} is null                                      then 'Unknown'
      else {{ title_case('delivery_status') }}
    end as delivery_status,

    case
      when lower(replace(trim(payment_method), ' ', '')) = 'paypal'       then 'PayPal'
      when lower(replace(trim(payment_method), ' ', '')) = 'creditcard'   then 'Credit Card'
      when lower(replace(trim(payment_method), ' ', '')) = 'debitcard'    then 'Debit Card'
      when lower(replace(trim(payment_method), ' ', '')) = 'banktransfer' then 'Bank Transfer'
      when lower(replace(trim(payment_method), ' ', '')) = 'ussd'         then 'USSD'
      when lower(replace(trim(payment_method), ' ', '')) = 'cash'         then 'Cash'
      when {{ clean_text('payment_method') }} is null                     then 'Unknown'
      else {{ title_case('payment_method') }}
    end as payment_method,

    {{ title_case(clean_text('region')) }}          as region,

    case
      when lower(replace(trim(sales_channel), ' ', '')) in ('in-store','instore') then 'In-Store'
      when {{ clean_text('sales_channel') }} is null then 'Unknown'
      else {{ title_case('sales_channel') }}
    end as sales_channel,

    {{ clean_text('notes') }}            as notes,
    {{ clean_text('campaign_id') }}      as campaign_id,
    {{ title_case(clean_text('shipping_city')) }} as shipping_city,
    upper({{ clean_text('warehouse_id') }})       as warehouse_id,
    {{ title_case(clean_text('order_priority')) }} as order_priority,
    {{ clean_text('return_reason') }}        as return_reason,
    {{ clean_text('cancellation_reason') }}  as cancellation_reason,

    case when lower(trim(return_requested)) in ('y','yes','true','1') then true
         when lower(trim(return_requested)) in ('n','no','false','0') then false
    end as return_requested,

    -- change tracking
    cast(created_at as timestamp)                                        as created_at,
    cast(updated_at as timestamp)                                        as updated_at,
    coalesce(cast(updated_at as timestamp), cast(created_at as timestamp)) as record_updated_at,

    -- unparsed-date flags: a value existed but the macro could not read it
    ({{ clean_text('order_date') }} is not null
       and {{ parse_mixed_date('order_date') }} is null)                 as invalid_order_date,
    ({{ clean_text('estimated_delivery') }} is not null
       and {{ parse_mixed_date('estimated_delivery') }} is null)         as invalid_estimated_delivery,
    ({{ clean_text('actual_delivery') }} is not null
       and {{ parse_mixed_date('actual_delivery') }} is null)            as invalid_actual_delivery
from source_sales