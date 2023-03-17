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
        where 1 = 1 and dd.date >= '10/16/2022' and dd.date <= current_date()

    ),
    _truck_do as (
        select
            dh.distribution_date,
            dh.truck_id,
            dd.item_id,
            dd.expiration_date,
            dd.po_id,
            dd.quantity
        from {{ ref("stg_distribution_header") }} dh
        join {{ ref("stg_distribution_detail") }} dd on dh.dh_id = dd.dh_id

    )

select
    aitad.date,
    aitad.truck_id,
    aitad.item_id,
    td.expiration_date,
    zeroifnull(td.quantity) as quantity_added,
    aitad.* exclude (date, truck_id, item_id)
from _all_items_trucks_and_dates aitad
left join
    _truck_do td
    on aitad.date = td.distribution_date
    and aitad.truck_id = td.truck_id
    and aitad.item_id = td.item_id
