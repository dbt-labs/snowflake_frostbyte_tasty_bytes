
with
    source as (
        select *
        from {{ source("supply_chain", "eod_stock_assignment") }}
    ),

    renamed as (
        select
            assignment_id as assignment_id,
            truck_id as truck_id,
            item_id as item_id,
            po_id as po_id,
            quantity as quantity,
            expiration_date as expiration_date,
            created_date as created_date
        from source
    )
    
select *
from renamed
