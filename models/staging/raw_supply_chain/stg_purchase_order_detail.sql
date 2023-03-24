
with
    source as (
        select *
        from {{ source("raw_supply_chain", "purchase_order_detail") }}
    ),

    renamed as (
        select
            po_detail_id as po_detail_id,
            po_id as po_id,
            warehouse_id as warehouse_id,
            vendor_id as vendor_id,
            item_id as item_id,
            line_item_id as line_item_id,
            quantity as quantity,
            manufacturing_date as manufacturing_date,
            expiration_date as expiration_date,
            unit_price as unit_price,
            created_date as created_date,
            updated_date as updated_date
        from source
    )
    
select *
from renamed
