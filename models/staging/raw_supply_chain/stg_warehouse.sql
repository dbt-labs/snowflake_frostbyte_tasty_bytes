
with
    source as (
        select *
        from {{ source("raw_supply_chain", "warehouse") }}
    ),

    renamed as (
        select
            warehouse_id as warehouse_id,
            name as warehouse_name,
            city as warehouse_city,
            country as warehouse_country,
            address as warehouse_address,
            latitude as warehouse_latitude,
            longitude as warehouse_longitude
        from source
    )
    
select *
from renamed
