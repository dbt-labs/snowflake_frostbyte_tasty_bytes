
with cybersyn_holiday as (

    select * from {{ ref('stg_cybersyn_holiday') }}

),

countries as (

    select * from {{ ref('stg_country') }}
),

final as (
    select 
        cybersyn_holiday.date,
        countries.country,
        cybersyn_holiday.holiday_name,
        1 AS holiday_flag
    FROM cybersyn_holiday
    JOIN countries
        ON cybersyn_holiday.iso_alpha2 = countries.iso_country
    WHERE cybersyn_holiday.date >= '01/01/2019' -- first day of Tasty Bytes Sales
        AND cybersyn_holiday.is_financial = TRUE -- federal/bank level holiday
        AND cybersyn_holiday.subdivision IS NULL -- only country level holiday
)

select 
    *
from
    final
