
with safegraph as (

    select * from {{ ref('stg_safegraph') }}

),
location as (

    select * from {{ ref('stg_location') }}
),
final as (
    select 
        l.location_id,
        sg.placekey,
        sg.location_name,
        sg.latitude,
        sg.longitude,
        sg.top_category,
        sg.sub_category,
        sg.street_address,
        sg.city,
        sg.country,
        l.region,
        l.iso_country_code
    from safegraph sg
    join location l
    on sg.location_id = l.location_id
)
select 
    *
from
    final
order by
    location_id