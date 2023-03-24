{{
    config(
        materialized = 'view'
    )
}}

with cybersyn_holiday as (

    select * from {{ ref('stg_cybersyn_holiday') }}

),
country as (

    select * from {{ ref('stg_country') }}
),
final as (
    select 
        ph.date,
        ts.country,
        ph.holiday_name,
        1 AS holiday_flag
    FROM cybersyn_holiday ph
    JOIN country ts
        ON ph.iso_alpha2 = ts.iso_country
    WHERE ph.date >= '01/01/2019' -- first day of Tasty Bytes Sales
        AND ph.is_financial = TRUE -- federal/bank level holiday
        AND ph.subdivision IS NULL -- only country level holiday
)
select 
    *
from
    final
order by
    date