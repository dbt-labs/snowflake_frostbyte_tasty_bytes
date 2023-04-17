{{
  config(
    schema = 'analytics'
    )
}}


with cybersyn_holiday as (

    select * from {{ ref('stg_cybersyn__holidays') }}

),

countries as (

    select * from {{ ref('stg_pos__countries') }}
),

final as (
    select cybersyn_holiday.date,
           countries.country,
           countries.city,
           cybersyn_holiday.holiday_name,
           1 AS holiday_flag
      from cybersyn_holiday
      join countries
        on cybersyn_holiday.iso_alpha2 = countries.iso_country
     where cybersyn_holiday.date >= '01/01/2019' -- first day of Tasty Bytes Sales
       and cybersyn_holiday.is_financial = TRUE -- federal/bank level holiday
       and cybersyn_holiday.subdivision IS NULL -- only country level holiday
)

select 
    *
from
    final
