with s as (select count(*) as n from {{ ref('stg_sales') }}),
     f as (select count(*) as n from {{ ref('fct_sales_profitability') }})
select s.n as stg_rows, f.n as fact_rows
from s, f
where s.n <> f.n