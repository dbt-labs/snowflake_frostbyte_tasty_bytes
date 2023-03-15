{{ config(materialized="table", sort="warehouse_id", dist="warehouse_id") }}

with
    source as (
        select warehouse_id, name, city, country, address, latitude, longitude
        from {{ source("raw_supply_chain", "warehouse") }}
    ),
    renamed as (
        select


            warehouse_id as warehouse_id,
            name as name,
            city as city,
            country as country,
            address as address,
            latitude as latitude,
            longitude as longitude
        from source
    )
select *
from renamed
