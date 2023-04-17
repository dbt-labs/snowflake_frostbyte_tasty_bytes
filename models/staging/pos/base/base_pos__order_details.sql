
with
    source as (
        select *
          from {{ source("pos", "order_detail") }}
    ),

    renamed as (
        select order_detail_id as order_detail_id,
               order_id as order_id,
               menu_item_id as menu_item_id,
               discount_id as discount_id,
               line_number as line_number,
               quantity as quantity,
               unit_price as unit_price,
               price as price,
               order_item_discount_amount as order_item_discount_amount
        from source
    )

select *
from renamed
