{config(materialized="view"}

CREATE OR REPLACE VIEW frostbyte_tasty_bytes.harmonized.truck_inbound_v
COMMENT = 'Truck Inbound Inventory Distribution View'
    AS
WITH _all_items_and_trucks AS 
(
SELECT DISTINCT
    t.*,
    i.*
FROM frostbyte_tasty_bytes.raw_pos.truck t
JOIN frostbyte_tasty_bytes.raw_pos.menu m
ON m.menu_type_id = t.menu_type_id
JOIN frostbyte_tasty_bytes.raw_supply_chain.recipe r
	ON r.menu_item_id = m.menu_item_id
JOIN frostbyte_tasty_bytes.raw_supply_chain.item i
	ON i.item_id = r.item_id
),
_all_items_trucks_and_dates AS
(
SELECT 
    dd.date,
    aitad.truck_id,
    aitad.item_id,
    aitad.* EXCLUDE (truck_id, item_id)
FROM _all_items_and_trucks aitad
JOIN frostbyte_tasty_bytes.raw_supply_chain.dim_date dd
WHERE 1=1
    AND dd.date >= '10/16/2022' --- first day we have truck level sample data
    AND dd.date <= CURRENT_DATE()
),
_truck_do AS
(
SELECT 
    dh.distribution_date,
    dh.truck_id,
    dd.item_id,
    dd.expiration_date,
    dd.po_id,
    dd.quantity
FROM frostbyte_tasty_bytes.raw_supply_chain.distribution_header dh
JOIN frostbyte_tasty_bytes.raw_supply_chain.distribution_detail dd
    ON dh.dh_id = dd.dh_id
)
SELECT 
    aitad.date,
    aitad.truck_id,
    aitad.item_id,
    td.expiration_date,
    ZEROIFNULL(td.quantity) AS quantity_added,
    aitad.* EXCLUDE (date, truck_id, item_id)
FROM _all_items_trucks_and_dates aitad
LEFT JOIN _truck_do td
    ON aitad.date = td.distribution_date
    AND aitad.truck_id = td.truck_id
    AND aitad.item_id = td.item_id; 