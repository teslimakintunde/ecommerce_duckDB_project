{{ config(materialized='view') }}

select
    {{ clean_text('customer_id') }}  as customer_id,

    -- PII: hashed, never exposed downstream
    case when {{ clean_text('email') }} is not null
         then md5(lower(trim(email))) end            as email_hash,
    case when {{ clean_text('phone_number') }} is not null
         then md5(trim(phone_number)) end            as phone_hash,
    ({{ clean_text('email') }} is not null)          as has_email,

    {{ parse_mixed_date('signup_date') }}            as signup_date,

    case
      when lower(trim(gender)) in ('female','femal','femle','f') then 'Female'
      when lower(trim(gender)) in ('male','m','mal')             then 'Male'
      when lower(trim(gender)) in ('other','o')                  then 'Other'
      else 'Unknown'
    end as gender,

    {{ title_case(clean_text('city')) }}    as city,
    {{ title_case(clean_text('region')) }}  as customer_region,

    case
      when lower(trim(loyalty_tier)) in ('silver','silvr')     then 'Silver'
      when lower(trim(loyalty_tier)) in ('gold','gld')         then 'Gold'
      when lower(trim(loyalty_tier)) in ('platinum','platnum') then 'Platinum'
      when lower(trim(loyalty_tier)) in ('bronze','brnze')     then 'Bronze'
      when {{ clean_text('loyalty_tier') }} is null            then 'Unknown'
      else {{ title_case('loyalty_tier') }}
    end as loyalty_tier,

    case
      when lower(replace(trim(preferred_payment), ' ', '')) = 'creditcard'   then 'Credit Card'
      when lower(replace(trim(preferred_payment), ' ', '')) = 'debitcard'    then 'Debit Card'
      when lower(replace(trim(preferred_payment), ' ', '')) = 'banktransfer' then 'Bank Transfer'
      when lower(replace(trim(preferred_payment), ' ', '')) = 'paypal'       then 'PayPal'
      when lower(replace(trim(preferred_payment), ' ', '')) = 'cash'         then 'Cash'
      when {{ clean_text('preferred_payment') }} is null                     then 'Unknown'
      else {{ title_case('preferred_payment') }}
    end as preferred_payment,

    try_cast(age as integer)                                         as customer_age,
    coalesce({{ title_case(clean_text('customer_segment')) }}, 'Unknown')     as customer_segment,
    coalesce({{ title_case(clean_text('acquisition_channel')) }}, 'Unknown')  as acquisition_channel,
    {{ clean_text('acquisition_campaign_id') }}                      as acquisition_campaign_id,

    ({{ clean_text('signup_date') }} is not null
       and {{ parse_mixed_date('signup_date') }} is null)            as invalid_signup_date
from {{ source('raw', 'customers_raw') }}