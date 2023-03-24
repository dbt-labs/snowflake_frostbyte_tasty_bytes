
WITH _tasty_countries AS
(
SELECT DISTINCT 
    c.country,
    c.iso_country 
FROM {{ ref('stg_country') }} c
)
SELECT 
    ph.date,
    ts.country,
    ph.holiday_name,
    1 AS holiday_flag
FROM {{ ref('stg_cybersyn_holiday') }} ph
JOIN _tasty_countries ts
    ON ph.iso_alpha2 = ts.iso_country
WHERE ph.date >= '01/01/2019' -- first day of Tasty Bytes Sales
    AND ph.is_financial = TRUE -- federal/bank level holiday
    AND ph.subdivision IS NULL -- only country level holiday