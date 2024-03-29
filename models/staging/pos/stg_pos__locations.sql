
with
    source as (
        select *
        from {{ source("pos", "location") }}
    ),

    renamed as (
        select
            location_id as location_id,
            placekey as placekey,
            location as location,
            city as city,
            region as region,
            iso_country_code as iso_country_code,
            country as country
        from source
    )
    
select *
from renamed
