{{ config(materialized="view") }}

with
    source as (
        select
            id,
            iso_alpha2,
            date,
            holiday_name,
            subdivision,
            is_financial
        from {{ source("raw_marketplace_cybersyn", "public_holidays") }}
    ),
    renamed as (
        select
            id AS id,
            iso_alpha2 AS iso_alpha2,
            date AS date,
            holiday_name AS holiday_name,
            subdivision AS subdivision,
            is_financial AS is_financial
        from source
    )
select *
from renamed
