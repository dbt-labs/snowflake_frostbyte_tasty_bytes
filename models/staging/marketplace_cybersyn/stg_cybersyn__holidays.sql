
with
    source as (
        select *
          from {{ source("marketplace_cybersyn", "public_holiday_calendar") }}
    ),

    renamed as (
        select
            iso_alpha2 AS iso_alpha2,
            date AS date,
            holiday_name AS holiday_name,
            subdivision AS subdivision,
            is_financial AS is_financial
        from source
    )
    
select *
from renamed
