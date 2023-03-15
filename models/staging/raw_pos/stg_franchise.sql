{{ config(materialized="table", sort="franchise_id", dist="franchise_id") }}

with
    source as (
        select franchise_id, first_name, last_name, city, country, e_mail, phone_number
        from {{ source("raw_pos", "franchise") }}
    ),
    renamed as (
        select

            franchise_id as franchise_id,
            first_name as first_name,
            last_name as last_name,
            city as city,
            country as country,
            e_mail as e_mail,
            phone_number as phone_number
        from source
    )
select *
from renamed
