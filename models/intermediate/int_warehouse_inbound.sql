with all_warehouses_all_items_all_dates as
(

    select * from {{ ref('int_warehouse_items_dates') }}

),

warehouse_po as 
(
select
    purchase_order_summary.warehouse_id,
    purchase_order_summary.po_id,
    purchase_order_summary.vendor_id,
    purchase_order_summary.po_date,
    purchase_order_summary.po_total,
    purchase_order_summary.shipment_date,
    purchase_order_summary.delivery_date,
    purchase_order_summary.line_item_id,
    purchase_order_summary.item_id,
    purchase_order_summary.quantity,
    purchase_order_summary.manufacturing_date,
    purchase_order_summary.expiration_date
from {{ ref('stg_purchase_order_summary') }} purchase_order_summary

)
select awaiad.date,
       awaiad.warehouse_id,
       awaiad.item_id,
       warehouse_po.po_id,
       warehouse_po.expiration_date,
       ZEROIFNULL(warehouse_po.quantity) as quantity_added,
       awaiad.warehouse_name,
       awaiad.warehouse_city,
       awaiad.warehouse_country,
       awaiad.warehouse_address,
       awaiad.warehouse_latitude,
       awaiad.warehouse_longitude,
       awaiad.item_name,
       awaiad.item_category,
       awaiad.unit,
       awaiad.unit_price,
       awaiad.unit_currency,
       awaiad.shelf_life_days,
       awaiad.vendor_id,
       awaiad.image_url,
       awaiad.vendor_name,
       awaiad.vendor_category 
  from all_warehouses_all_items_all_dates awaiad
  left join warehouse_po 
    on warehouse_po.delivery_date = awaiad.date
   and warehouse_po.warehouse_id = awaiad.warehouse_id
   and warehouse_po.item_id = awaiad.item_id