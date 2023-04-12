
with
    source as (
        select *
        from {{ source("pos", "franchise") }}
    ),

    renamed as (
        select

            franchise_id as franchise_id,
            first_name as franchisee_first_name,
            last_name as franchisee_last_name,
            city as city,
            country as country,
            e_mail as e_mail,
            phone_number as phone_number
        from source
    )
    
select *
from renamed
