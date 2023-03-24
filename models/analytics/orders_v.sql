<<<<<<< HEAD
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
=======
{{
    config(
        materialized = 'view'
    )
}}

with hmz_holiday as (

    select * from {{ ref('hmz_holiday_v') }}

),
hmz_orders as (

    select * from {{ ref('hmz_orders_v') }}
),
hmz_safegraph as (

    select * from {{ ref('hmz_safegraph_poi') }}
),
final as (
    select 
        date(o.order_ts) as date,
        o.* exclude (country), 
        h.holiday_name,
        zeroifnull(h.holiday_flag) as holiday_flag,
        sp.* exclude (location_id, region)
    from hmz_orders o
    join hmz_safegraph sp
        on sp.location_id = o.location_id
        and sp.city = o.primary_city
        and sp.country = o.country
    left join hmz_holiday h
        on h.date = date(o.order_ts)
        and h.country = o.country
)
select 
    *
from
    final
order by
    date
>>>>>>> 33dd6fcc484f6fb5dc28e76f7c035e490f7da30a
