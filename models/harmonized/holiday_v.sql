{config(materialized="view"}

CREATE OR REPLACE VIEW frostbyte_tasty_bytes.harmonized.holiday_v
    AS
WITH _tasty_countries AS
(
SELECT DISTINCT 
    c.country,
    c.iso_country 
FROM frostbyte_tasty_bytes.raw_pos.country c
)
SELECT 
    ph.date,
    ts.country,
    ph.holiday_name,
    1 AS holiday_flag
FROM frostbyte_cs_public.cybersyn.public_holidays ph
JOIN _tasty_countries ts
    ON ph.iso_alpha2 = ts.iso_country
WHERE 1=1
    AND ph.date >= '01/01/2019' -- first day of Tasty Bytes Sales
    AND ph.is_financial = TRUE -- federal/bank level holiday
    AND ph.subdivision IS NULL -- only country level holiday
;