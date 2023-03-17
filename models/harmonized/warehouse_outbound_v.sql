with
    _all_warehouses_all_items as (
        select
            w.* rename name as warehouse_name,
            i.* rename(name as item_name, category as item_category),
            v.* exclude (vendor_id) rename name as vendor_name
        from {{ ref("stg_warehouse") }} w
        join {{ ref("stg_item") }} i
        join {{ ref("stg_vendor") }} v on i.category = v.category
    ),
    _all_warehouses_all_items_all_dates as (
        select
            dd.date,
            awai.warehouse_id,
            awai.warehouse_name,
            awai.city as warehouse_city,
            awai.country as warehouse_country,
            awai.address as warehouse_address,
            awai.latitude as warehouse_latitude,
            awai.longitude as warehouse_longitude,
            awai.item_id,
            awai.item_category,
            awai.category,
            awai.unit,
            awai.unit_price,
            awai.unit_currency,
            awai.shelf_life_days,
            awai.image_url,
            awai.vendor_id,
            awai.vendor_name
        from {{ ref("stg_calender") }} dd
        join _all_warehouses_all_items awai
        where 1 = 1 and dd.date >= '10/14/2022' and dd.date <= current_date()
    ),
    _warehouse_do as (

        select
            doh.warehouse_id,
            doh.dh_id,
            doh.distribution_date,
            dod.line_item_id,
            dod.item_id,
            dod.quantity,
            dod.expiration_date,
            dod.po_id
        from {{ ref("stg_distribution_header") }} doh
        join {{ ref("stg_distribution_detail") }} dod on doh.dh_id = dod.dh_id
    )
select
    awaiad.date,
    awaiad.warehouse_id,
    awaiad.item_id,
    awaiad.vendor_id,
    wd.po_id,
    wd.expiration_date,
    zeroifnull((wd.quantity * -1)) as quantity_removed,
    awaiad.* exclude (date, warehouse_id, item_id, vendor_id)
from _all_warehouses_all_items_all_dates awaiad
left join
    _warehouse_do wd
    on wd.distribution_date = awaiad.date
    and wd.warehouse_id = awaiad.warehouse_id
    and wd.item_id = awaiad.item_id
where 1 = 1
;
