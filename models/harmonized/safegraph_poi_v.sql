{config(materialized="view"}

CREATE OR REPLACE VIEW frostbyte_tasty_bytes.harmonized.safegraph_poi_v
    AS
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
FROM frostbyte_safegraph.public.frostbyte_tb_safegraph_s sg
JOIN frostbyte_tasty_bytes.raw_pos.location l 
ON sg.location_id = l.location_id;