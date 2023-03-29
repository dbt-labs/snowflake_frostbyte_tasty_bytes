{{
    config(
        materialized = "table",
        transient = false
    )
}}

with int_holiday as (

    select * from {{ ref('int_holiday') }}

),
hmz_orders as (

    select * from {{ ref('int_orders') }}
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
    left join int_holiday h
        on h.date = date(o.order_ts)
        and h.country = o.country
)
select 
    *
from
    final
order by
    date
