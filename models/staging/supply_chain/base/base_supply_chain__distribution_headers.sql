
with
    source as (
        select *
        from {{ source("supply_chain", "distribution_header") }}
    ),

    renamed as (
        select
            dh_id as dh_id,
            truck_id as truck_id,
            warehouse_id as warehouse_id,
            distribution_date as distribution_date,
            created_date as created_date,
            updated_date as updated_date
        from source
    )
    
select *
from renamed
