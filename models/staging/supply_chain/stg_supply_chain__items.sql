
with
    source as (
        select *
        from {{ source("supply_chain", "item") }}
    ),

    renamed as (
        select
            item_id as item_id,
            name as item_name,
            category as item_category,
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
