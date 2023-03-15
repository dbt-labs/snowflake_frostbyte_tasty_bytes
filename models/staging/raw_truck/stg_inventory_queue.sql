{{ config(materialized="table", sort="truck_id", dist="truck_id") }}

with
    source as (
        select


            truck_id,
            item_id,
            warehouse_id,
            quantity,
            schedule_week,
            created_date,
            updated_date,
            is_processed
        from {{ source("raw_truck", "inventory_queue") }}
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
