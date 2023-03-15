{{ config(materialized="table", sort="country_id", dist="country_id") }}

with
    source as (
        select

            country_id,
            country,
            iso_currency,
            iso_country,
            city_id,
            city,
            city_population
        from {{ source("raw_pos", "country") }}
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
