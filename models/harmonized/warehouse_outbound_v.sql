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
_warehouse_do AS 
(
SELECT 
  doh.warehouse_id,
  doh.dh_id,
  doh.distribution_date,
  dod.line_item_id,
  dod.item_id,
  dod.quantity,
  dod.expiration_date,
  dod.po_id
FROM {{ ref('stg_distribution_header') }} doh
JOIN {{ ref('stg_distribution_detail') }} dod
	ON doh.dh_id = dod.dh_id
)
SELECT 
    awaiad.date,
    awaiad.warehouse_id,
    awaiad.item_id,
    awaiad.vendor_id,
    wd.po_id,
    wd.expiration_date,
    ZEROIFNULL((wd.quantity * -1)) AS quantity_removed,
    awaiad.* EXCLUDE (date, warehouse_id, item_id, vendor_id)
FROM _all_warehouses_all_items_all_dates awaiad
LEFT JOIN _warehouse_do wd
    ON wd.distribution_date = awaiad.date
    AND wd.warehouse_id = awaiad.warehouse_id
    AND wd.item_id = awaiad.item_id
