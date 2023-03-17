with
    _all_items_and_trucks as (
        select distinct t.*, i.*
        from {{ ref("stg_truck") }} t
        join {{ ref("stg_menu") }} m on m.menu_type_id = t.menu_type_id
        join {{ ref("stg_recipe") }} r on r.menu_item_id = m.menu_item_id
        join {{ ref("stg_item") }} i on i.item_id = r.item_id
    ),
    _all_items_trucks_and_dates as (
        select
            dd.date, aitad.truck_id, aitad.item_id, aitad.* exclude (truck_id, item_id)
        from _all_items_and_trucks aitad
        join {{ ref("stg_calender") }} dd
        where 1 = 1 and dd.date <= current_date()

    ),
    _truck_item_id_usage as (
        select
            oh.truck_id,
            date(oh.order_ts) as date,
            i.item_id,
            sum(r.unit_quantity) as quantity_used
        from {{ ref("stg_order_header") }} oh
        join {{ ref("stg_order_detail") }} od on oh.order_id = od.order_id
        join {{ ref("stg_recipe") }} r on r.menu_item_id = od.menu_item_id
        join {{ ref("stg_item") }} i on i.item_id = r.item_id

        where 1 = 1 and date(oh.order_ts) >= '10/16/2022'
        -- first day we have inventory metrics in sample data
        group by oh.truck_id, date, i.item_id

    )

select
    aitad.date,
    aitad.truck_id,
    aitad.item_id,
    zeroifnull((tiiu.quantity_used * -1)) as quantity_used,
    aitad.* exclude (date, truck_id, item_id)
from _all_items_trucks_and_dates aitad
left join
    _truck_item_id_usage tiiu
    on aitad.date = tiiu.date
    and aitad.truck_id = tiiu.truck_id
    and aitad.item_id = tiiu.item_id
;
