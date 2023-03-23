{config(materialized="view"}

CREATE OR REPLACE VIEW frostbyte_tasty_bytes.analytics.orders_v
COMMENT = 'Tasty Bytes Order Detail View'
    AS
SELECT 
    DATE(o.order_ts) AS date,
    o.* EXCLUDE (country), 
    h.holiday_name,
    ZEROIFNULL(h.holiday_flag) AS holiday_flag,
    sp.* EXCLUDE (location_id)
FROM frostbyte_tasty_bytes.harmonized.orders_v o
LEFT JOIN frostbyte_tasty_bytes.harmonized.holiday_v h
    ON h.date = DATE(o.order_ts)
    AND h.country = o.country
JOIN frostbyte_tasty_bytes.harmonized.safegraph_poi_v sp
    ON sp.location_id = o.location_id
;