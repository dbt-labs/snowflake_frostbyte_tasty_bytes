
with
    source as (
        select *
          from {{ source("supply_chain", "vendor") }}
    ),

    renamed as (
        select vendor_id as vendor_id, 
               name as vendor_name, 
               category as vendor_category 
          from source
    )
    
select *
from renamed
