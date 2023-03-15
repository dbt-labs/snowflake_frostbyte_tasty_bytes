{{ config(materialized="table", sort="menu_id", dist="menu_id") }}

with
    source as (
        select
            menu_id,
            menu_type_id,
            menu_type,
            truck_brand_name,
            menu_item_id,
            menu_item_name,
            item_category,
            item_subcategory,
            cost_of_goods_usd,
            sale_price_usd,
            menu_item_health_metrics_obj
        from {{ source("raw_pos", "menu") }}
    ),
    renamed as (
        select
            menu_id as menu_id,
            menu_type_id as menu_type_id,
            menu_type as menu_type,
            truck_brand_name as truck_brand_name,
            menu_item_id as menu_item_id,
            menu_item_name as menu_item_name,
            item_category as item_category,
            item_subcategory as item_subcategory,
            cost_of_goods_usd as cost_of_goods_usd,
            sale_price_usd as sale_price_usd,
            menu_item_health_metrics_obj as menu_item_health_metrics_obj
        from source
    )
select *
from renamed
