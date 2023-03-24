
with
    source as (
        select *
          from {{ source("raw_supply_chain", "vendor") }}
    ),

    renamed as (
        select vendor_id as vendor_id, 
               name as name, 
               category as category 
          from source
    )
    
select *
from renamed
