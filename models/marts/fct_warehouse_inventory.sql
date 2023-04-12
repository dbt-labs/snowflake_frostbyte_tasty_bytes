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
from {{ ref('stg_supply_chain__purchase_order_summary') }} purchase_order_summary

),

warehouse_do AS 
(
select warehouse_id,
       dh_id,
       distribution_date,
       line_item_id,
       item_id,
       quantity,
       expiration_date,
       po_id
from {{ ref('stg_supply_chain__distribution_summary') }}
)

select awaiad.date,
       awaiad.warehouse_id,
       awaiad.item_id,
       warehouse_po.po_id,
       warehouse_po.expiration_date as quantity_added_expiration_date,
       zeroifnull(warehouse_po.quantity) as quantity_added,
       warehouse_do.expiration_date as quantity_removed_expiration_date,
       zeroifnull((warehouse_do.quantity * -1)) as quantity_removed,
       sum(quantity_added + quantity_removed) 
       over (partition by awaiad.warehouse_id, awaiad.item_id order by awaiad.date) as item_id_quantity_in_stock,
       (item_id_quantity_in_stock * awaiad.unit_price) as cost_of_inventory,
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
  from all_warehouses_all_items_all_dates as awaiad
  left join warehouse_po 
    on warehouse_po.delivery_date = awaiad.date
   and warehouse_po.warehouse_id = awaiad.warehouse_id
   and warehouse_po.item_id = awaiad.item_id
  left join warehouse_do
    on warehouse_do.distribution_date = awaiad.date
   and warehouse_do.warehouse_id = awaiad.warehouse_id
   and warehouse_do.item_id = awaiad.item_id