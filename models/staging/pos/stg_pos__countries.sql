
with
    source as (
        select *
        from {{ source("pos", "country") }}
    ),

    renamed as (
        select
            country_id as country_id,
            country as country,
            iso_currency as iso_currency,
            iso_country as iso_country,
            city_id as city_id,
            city as city,
            city_population as city_population
        from source
    )
    
select *
from renamed
