{{ config(materialized='view') }}

select
    {{ clean_text('product_id') }}        as product_id,
    {{ clean_text('product_name') }}      as product_name,
    {{ clean_text('category') }}          as product_category,
    {{ parse_mixed_date('launch_date') }} as launch_date,

    try_cast(base_price as decimal(12,2)) as base_price,
    try_cast(unit_cost as decimal(12,2))  as unit_cost,

    {{ clean_text('supplier_code') }}     as supplier_code,
    {{ clean_text('country_of_origin') }} as country_of_origin,

    case when lower(trim(is_active)) in ('yes','y','true','1') then true
         when lower(trim(is_active)) in ('no','n','false','0') then false
    end as is_active,

    try_cast(reorder_level as integer)        as reorder_level,
    try_cast(current_stock_level as integer)  as current_stock_level,
    try_cast(weight_kg as decimal(10,2))      as weight_kg,
    {{ clean_text('seasonality_tag') }}       as seasonality_tag,

    ({{ clean_text('launch_date') }} is not null
       and {{ parse_mixed_date('launch_date') }} is null) as invalid_launch_date
from {{ source('raw', 'products_raw') }}