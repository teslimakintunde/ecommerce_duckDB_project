{{ config(severity='warn') }}
select line_item_id, order_date, cast(created_at as date) as created_date
from {{ ref('stg_sales') }}
where order_date is not null and order_date <> cast(created_at as date)