{{ config(materialized="table", sort="recipe_id", dist="recipe_id") }}

with
    source as (
        select recipe_id, menu_item_id, menu_item_line_item, item_id, unit_quantity
        from {{ source("raw_supply_chain", "recipe") }}
    ),
    renamed as (
        select




            recipe_id as recipe_id,
            menu_item_id as menu_item_id,
            menu_item_line_item as menu_item_line_item,
            item_id as item_id,
            unit_quantity as unit_quantity
        from source
    )
select *
from renamed
