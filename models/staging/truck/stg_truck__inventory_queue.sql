
with
    source as (
        select *
        from {{ source("truck", "inventory_queue") }}
    ),

    renamed as (
        select
            truck_id as truck_id,
            item_id as item_id,
            warehouse_id as warehouse_id,
            quantity as quantity,
            schedule_week as schedule_week,
            created_date as created_date,
            updated_date as updated_date,
            is_processed as is_processed
        from source
    )
    
select *
from renamed
