WITH _all_warehouses_all_items AS
(
SELECT 
    w.* RENAME name AS warehouse_name,
    i.* RENAME (name AS item_name, category AS item_category),
    v.* EXCLUDE (vendor_id) RENAME name AS vendor_name
FROM {{ ref('stg_warehouse') }} w
JOIN {{ ref('stg_item') }} i
JOIN {{ ref('stg_vendor') }} v
    ON i.category = v.category
),

_all_warehouses_all_items_all_dates AS
(
SELECT 
    dd.date,
    awai.warehouse_id,
    awai.warehouse_name,
    awai.city AS warehouse_city,
    awai.country AS warehouse_country,
    awai.address AS warehouse_address,
    awai.latitude AS warehouse_latitude,
    awai.longitude AS warehouse_longitude,
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
FROM {{ ref('stg_dim_date') }} dd
JOIN _all_warehouses_all_items awai
WHERE dd.date >= '10/14/2022'
    AND dd.date <= CURRENT_DATE()
),
_warehouse_po AS 
(
SELECT
    poh.warehouse_id,
    poh.po_id,
    poh.vendor_id,
    v.name AS vendor_name,
    v.category AS vendor_category,
    poh.po_date,
    poh.po_total,
    poh.shipment_date,
    poh.delivery_date,
    pod.line_item_id,
    pod.item_id,
    pod.quantity,
    pod.manufacturing_date,
    pod.expiration_date
FROM {{ ref('stg_purchase_order_header') }} poh
JOIN {{ ref('stg_purchase_order_detail') }} pod
	ON poh.po_id = pod.po_id
JOIN {{ ref('stg_vendor') }} v
	ON v.vendor_id = poh.vendor_id
)
SELECT 
    awaiad.date,
    awaiad.warehouse_id,
    awaiad.item_id,
    wp.po_id,
    wp.expiration_date,
    ZEROIFNULL(wp.quantity) AS quantity_added,
    awaiad.* EXCLUDE (date, warehouse_id, item_id, vendor_id)
FROM _all_warehouses_all_items_all_dates awaiad
LEFT JOIN _warehouse_po wp
    ON wp.delivery_date = awaiad.date
    AND wp.warehouse_id = awaiad.warehouse_id
    AND wp.item_id = awaiad.item_id