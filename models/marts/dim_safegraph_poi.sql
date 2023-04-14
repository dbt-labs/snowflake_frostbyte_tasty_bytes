
with safegraph as (

    select * from {{ ref('stg_safegraph__safegraph') }}

),
location as (

    select * from {{ ref('stg_pos__locations') }}
),
final as (
    select location.location_id,
           safegraph.placekey,
           safegraph.location_name,
           safegraph.latitude,
           safegraph.longitude,
           safegraph.top_category,
           safegraph.sub_category,
           safegraph.street_address,
           safegraph.city,
           safegraph.country,
           location.region,
           location.iso_country_code
      from safegraph
      join location
        on safegraph.location_id = location.location_id
)
select 
    *
from
    final
