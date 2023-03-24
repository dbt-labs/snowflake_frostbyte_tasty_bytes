
SELECT 
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
FROM {{ ref('stg_safegraph') }} sg
JOIN {{ ref('stg_location') }} l 
ON sg.location_id = l.location_id