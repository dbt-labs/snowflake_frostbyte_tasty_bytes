
with
    source as (
        select *
          from {{ source("pos", "truck") }}
    ),

    renamed as (
        select
            truck_id as truck_id,
            menu_type_id as menu_type_id,
            primary_city as primary_city,
            region as region,
            iso_region as iso_region,
            country as country,
            iso_country_code as iso_country_code,
            franchise_flag as franchise_flag,
            year as year,
            make as make,
            model as model,
            ev_flag as ev_flag,
            franchise_id as franchise_id,
            truck_opening_date as truck_opening_date
        from source
    )
    
select *
from renamed
