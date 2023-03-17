{{ config(materialized="view", sort="item_id", dist="item_id") }}

with
    source as (
        select
            item_id,
            name,
            category,
            unit,
            unit_price,
            unit_currency,
            shelf_life_days,
            vendor_id,
            image_url
        from {{ source("raw_supply_chain", "item") }}
    ),
    renamed as (
        select
            item_id as item_id,
            name as name,
            category as category,
            unit as unit,
            unit_price as unit_price,
            unit_currency as unit_currency,
            shelf_life_days as shelf_life_days,
            vendor_id as vendor_id,
            image_url as image_url
        from source
    )
select *
from renamed
