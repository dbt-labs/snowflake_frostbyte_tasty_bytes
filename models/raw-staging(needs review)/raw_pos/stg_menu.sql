with
source as (
    select * from {{ source('raw_pos','menu') }}
),
renamed as (
    Select * from source
)
select * from renamed