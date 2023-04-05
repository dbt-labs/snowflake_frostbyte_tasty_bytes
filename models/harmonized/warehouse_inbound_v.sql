with all_warehouses_all_items_all_dates as
(

    select * from {{ ref('int_warehouse_items_dates') }}

),

_warehouse_po AS 
(
SELECT
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
FROM {{ ref('stg_purchase_order_summary') }} purchase_order_summary

)
SELECT 
    awaiad.date,
    awaiad.warehouse_id,
    awaiad.item_id,
    wp.po_id,
    wp.expiration_date,
    ZEROIFNULL(wp.quantity) AS quantity_added,
    awaiad.* EXCLUDE (date, warehouse_id, item_id, vendor_id)
FROM all_warehouses_all_items_all_dates awaiad
LEFT JOIN _warehouse_po wp
    ON wp.delivery_date = awaiad.date
    AND wp.warehouse_id = awaiad.warehouse_id
    AND wp.item_id = awaiad.item_id