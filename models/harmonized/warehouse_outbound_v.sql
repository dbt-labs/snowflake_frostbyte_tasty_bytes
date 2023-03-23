{config(materialized="view"}

CREATE OR REPLACE VIEW frostbyte_tasty_bytes.harmonized.warehouse_outbound_v
COMMENT = 'Warehouse Outbound Purchase Order View'
   AS
WITH _all_warehouses_all_items AS
(
SELECT 
    w.* RENAME name AS warehouse_name,
    i.* RENAME (name AS item_name, category AS item_category),
    v.* EXCLUDE (vendor_id) RENAME name AS vendor_name
FROM frostbyte_tasty_bytes.raw_supply_chain.warehouse w
JOIN frostbyte_tasty_bytes.raw_supply_chain.item i
JOIN frostbyte_tasty_bytes.raw_supply_chain.vendor v
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
FROM frostbyte_tasty_bytes.raw_supply_chain.dim_date dd
JOIN _all_warehouses_all_items awai
WHERE 1=1
    AND dd.date >= '10/14/2022'
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
FROM frostbyte_tasty_bytes.raw_supply_chain.distribution_header doh
JOIN frostbyte_tasty_bytes.raw_supply_chain.distribution_detail dod
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
WHERE 1=1;