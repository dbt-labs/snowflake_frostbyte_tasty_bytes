
with
    source as (
        select *
          from {{ source("marketplace_safegraph", "frostbyte_tb_safegraph_s") }}
    ),
    
    renamed as (
        select
            placekey,
            parent_placekey as parent_placekey,
            safegraph_brand_ids as safegraph_brand_ids,
            location_name as location_name,
            brands as brands,
            store_id as store_id,
            top_category as top_category,
            sub_category as sub_category,
            naics_code as naics_code,
            latitude as latitude,
            longitude as longitude,
            street_address as street_address,
            city as city,
            region as region,
            postal_code as postal_code,
            open_hours as open_hours,
            category_tags as category_tags,
            opened_on as opened_on,
            closed_on as closed_on,
            tracking_closed_since as tracking_closed_since,
            geometry_type as geometry_type,
            polygon_wkt as polygon_wkt,
            polygon_class as polygon_class,
            enclosed as enclosed,
            phone_number as phone_number,
            is_synthetic as is_synthetic,
            includes_parking_lot as includes_parking_lot,
            wkt_area_sq_meters as wkt_area_sq_meters,
            country as country,
            location_id as location_id
        from source
    )

select *
from renamed
