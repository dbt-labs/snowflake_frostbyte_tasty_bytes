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
    _warehouse_po as (
        select
            poh.warehouse_id,
            poh.po_id,
            poh.vendor_id,
            v.name as vendor_name,
            v.category as vendor_category,
            poh.po_date,
            poh.po_total,
            poh.shipment_date,
            poh.delivery_date,
            pod.line_item_id,
            pod.item_id,
            pod.quantity,
            pod.manufacturing_date,
            pod.expiration_date
        from {{ ref("stg_purchase_order_header") }} poh
        join {{ ref("stg_purchase_order_detail") }} pod on poh.po_id = pod.po_id
        join {{ ref("stg_vendor") }} v on v.vendor_id = poh.vendor_id
    )
select
    awaiad.date,
    awaiad.warehouse_id,
    awaiad.item_id,
    wp.po_id,
    wp.expiration_date,
    zeroifnull(wp.quantity) as quantity_added,
    awaiad.* exclude (date, warehouse_id, item_id, vendor_id)
from _all_warehouses_all_items_all_dates awaiad
left join
    _warehouse_po wp
    on wp.delivery_date = awaiad.date
    and wp.warehouse_id = awaiad.warehouse_id
    and wp.item_id = awaiad.item_id
;
